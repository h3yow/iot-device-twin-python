"""升级：重复下发约束、中断（失败/超时）识别、批次熔断与版本门限。"""
import threading

import pytest

from app.models import BatchStatus, PauseReason, TERMINAL_UPGRADE_STATES, UpgradeStatus
from app.storage import Storage
from app.upgrade import UpgradeError, UpgradeService


@pytest.fixture()
def svc(tmp_path):
    storage = Storage(str(tmp_path / "up.db"))
    now = [1000.0]

    def clock():
        now[0] += 0.001  # 单调推进，保证批次创建时间可区分先后
        return now[0]

    service = UpgradeService(storage, clock=clock)
    service.create_batch("b1", to_version="2.0.0", min_from_version="1.0.0",
                         max_failure_ratio=0.5)
    yield service, now
    storage.close()


def test_concurrent_dispatch_allows_single_active_task(svc):
    service, _ = svc
    results, errors = [], []
    barrier = threading.Barrier(10)

    def worker():
        barrier.wait()
        try:
            results.append(service.dispatch("b1", "d1", "1.2.0"))
        except UpgradeError as e:
            errors.append(str(e))

    threads = [threading.Thread(target=worker) for _ in range(10)]
    for t in threads: t.start()
    for t in threads: t.join()

    print(f"[判定依据] 10 线程并发对 d1 下发：成功={len(results)} 拒绝={len(errors)}，"
          f"拒绝样例={errors[0] if errors else None}")
    assert len(results) == 1 and len(errors) == 9  # 同设备至多一个进行中任务


def test_version_gate_cannot_be_bypassed(svc):
    service, _ = svc
    with pytest.raises(UpgradeError, match="below gate"):
        service.dispatch("b1", "d-old", "0.9.9")
    task = service.dispatch("b1", "d-ok", "1.0.0")  # 恰好等于门限允许
    print(f"[判定依据] 门限=1.0.0：0.9.9 被拒，1.0.0 放行 task={task.task_id[:8]}")


def test_failed_device_identified_and_batch_paused(svc):
    service, _ = svc
    t1 = service.dispatch("b1", "d1", "1.2.0")
    t2 = service.dispatch("b1", "d2", "1.3.0")
    t3 = service.dispatch("b1", "d3", "1.4.0")
    service.report_progress(t1.task_id, UpgradeStatus.DOWNLOADING)
    service.report_progress(t1.task_id, UpgradeStatus.FLASHING)
    service.report_progress(t1.task_id, UpgradeStatus.SUCCESS)
    service.report_progress(t2.task_id, UpgradeStatus.FAILED)
    service.report_progress(t3.task_id, UpgradeStatus.FAILED)

    batch = service._storage.load_batch("b1")
    failed = [t.device_id for t in service._storage.load_tasks("b1")
              if t.status is UpgradeStatus.FAILED]
    print(f"[判定依据] 3 任务中失败={failed}，失败率 2/3 > 门限 0.5 -> 批次={batch.status.value}")
    assert failed == ["d2", "d3"]
    assert batch.status is BatchStatus.PAUSED
    with pytest.raises(UpgradeError, match="dispatch stopped"):
        service.dispatch("b1", "d4", "1.5.0")  # 熔断后禁止继续推进


def test_interrupted_upgrade_times_out_and_device_can_redispatch_in_new_batch(svc):
    service, now = svc
    # 独立批次，失败率门限 1.0（不熔断），专注验证超时判定与重下发
    service.create_batch("b-retry", to_version="2.0.0", min_from_version="1.0.0",
                         max_failure_ratio=1.0)
    t = service.dispatch("b-retry", "d1", "1.2.0")
    service.report_progress(t.task_id, UpgradeStatus.DOWNLOADING)
    now[0] += 7200  # 设备中断，再无回报

    timed_out = service.check_timeouts(now[0], timeout_seconds=3600)
    print(f"[判定依据] 任务 {t.task_id[:8]} 停滞 7200s > 超时 3600s -> "
          f"status={timed_out[0].status.value}")
    assert [x.task_id for x in timed_out] == [t.task_id]
    assert timed_out[0].status is UpgradeStatus.TIMEOUT

    # 新语义：同一批次内超时设备不再被重复下发
    with pytest.raises(UpgradeError, match="already dispatched"):
        service.dispatch("b-retry", "d1", "1.2.0")

    # 旧批次收尾（全部任务终态）后名额释放，可在新批次重新下发（如修复后重试）
    service.close_batch("b-retry")
    service.create_batch("b-retry-2", to_version="2.1.0", min_from_version="1.0.0",
                         max_failure_ratio=1.0)
    t2 = service.dispatch("b-retry-2", "d1", "1.2.0")
    assert t2.task_id != t.task_id
    print(f"[判定依据] 超时设备在同批次内拒绝重发；旧批次 completed 释放名额后，"
          f"新批次 b-retry-2 下发新任务 {t2.task_id[:8]}")


def test_terminal_task_rejects_further_progress(svc):
    service, _ = svc
    t = service.dispatch("b1", "d1", "1.2.0")
    service.report_progress(t.task_id, UpgradeStatus.FAILED)
    with pytest.raises(UpgradeError, match="terminal"):
        service.report_progress(t.task_id, UpgradeStatus.DOWNLOADING)
    print(f"[判定依据] 终态任务 {t.task_id[:8]} 拒绝再次迁移，状态保持 failed")


def test_pause_blocks_dispatch_but_inflight_runs_and_resume_continues(svc):
    service, _ = svc
    t1 = service.dispatch("b1", "d1", "1.2.0")
    t2 = service.dispatch("b1", "d2", "1.3.0")
    service.report_progress(t1.task_id, UpgradeStatus.DOWNLOADING)  # d1 进行中

    # 暂停：不能再往任何设备继续下发
    batch = service.pause_batch("b1")
    assert batch.status is BatchStatus.PAUSED
    assert batch.pause_reason is PauseReason.MANUAL
    with pytest.raises(UpgradeError, match="dispatch stopped"):
        service.dispatch("b1", "d3", "1.4.0")
    # 已经在跑的设备允许自然结束
    service.report_progress(t1.task_id, UpgradeStatus.FLASHING)
    service.report_progress(t1.task_id, UpgradeStatus.SUCCESS)
    service.report_progress(t2.task_id, UpgradeStatus.DOWNLOADING)
    service.report_progress(t2.task_id, UpgradeStatus.FLASHING)
    service.report_progress(t2.task_id, UpgradeStatus.SUCCESS)

    # 恢复：接着原进度推进
    batch = service.resume_batch("b1")
    assert batch.status is BatchStatus.RUNNING and batch.pause_reason is None
    t3 = service.dispatch("b1", "d3", "1.4.0")  # 未下发过的新设备可以推进
    # 已完成设备不会因暂停/恢复被重新下发
    with pytest.raises(UpgradeError, match="already dispatched"):
        service.dispatch("b1", "d1", "1.2.0")
    with pytest.raises(UpgradeError, match="already dispatched"):
        service.dispatch("b1", "d2", "1.3.0")
    print(f"[判定依据] 暂停后 d3 被挡、d1 自然跑到 success；恢复后仅 d3 拿到任务 "
          f"{t3.task_id[:8]}，d1/d2 未被重复下发")

    # 状态非法的暂停/恢复
    with pytest.raises(UpgradeError, match="only paused can resume"):
        service.resume_batch("b1")
    service.pause_batch("b1")
    service.report_progress(t3.task_id, UpgradeStatus.FAILED)
    service.resume_batch("b1")
    service.close_batch("b1")
    with pytest.raises(UpgradeError, match="already completed"):
        service.pause_batch("b1")


def test_circuit_breaker_pause_is_distinguishable_and_failed_devices_never_redispatched(svc):
    service, now = svc
    service.create_batch("b-cb", to_version="2.0.0", min_from_version="1.0.0",
                         max_failure_ratio=0.5)
    t1 = service.dispatch("b-cb", "d1", "1.2.0")
    t2 = service.dispatch("b-cb", "d2", "1.3.0")
    t3 = service.dispatch("b-cb", "d3", "1.4.0")
    service.report_progress(t1.task_id, UpgradeStatus.DOWNLOADING)
    service.report_progress(t1.task_id, UpgradeStatus.FLASHING)
    service.report_progress(t1.task_id, UpgradeStatus.SUCCESS)
    service.report_progress(t2.task_id, UpgradeStatus.FAILED)
    now[0] += 4000
    service.check_timeouts(now[0], timeout_seconds=3600)  # d3 超时

    batch = service._storage.load_batch("b-cb")
    assert batch.status is BatchStatus.PAUSED
    assert batch.pause_reason is PauseReason.CIRCUIT_BREAKER  # 明确是熔断而非手动/完成
    assert batch.status is not BatchStatus.COMPLETED
    with pytest.raises(UpgradeError, match="circuit_breaker"):
        service.dispatch("b-cb", "d4", "1.5.0")

    # 恢复推进：未处理过的 d4 可以下发；失败/超时设备在该批次内永不重发
    service.resume_batch("b-cb")
    service.dispatch("b-cb", "d4", "1.5.0")
    with pytest.raises(UpgradeError, match="already dispatched"):
        service.dispatch("b-cb", "d2", "1.3.0")  # failed
    with pytest.raises(UpgradeError, match="already dispatched"):
        service.dispatch("b-cb", "d3", "1.4.0")  # timeout
    print("[判定依据] 1 success + 1 failed + 1 timeout，失败率 2/3 > 0.5 -> "
          "PAUSED(circuit_breaker)；恢复后 d4 可推进，d2/d3 不再收到任务")


def test_cross_batch_device_contention_priority_and_capacity_release(svc):
    service, _ = svc
    # b-low 先创建且 RUNNING，已占用 d1（任务失败终态，名额仍归属该批次）
    service.create_batch("b-low", to_version="2.0.0", min_from_version="1.0.0",
                         max_failure_ratio=1.0, priority=1)
    low_t = service.dispatch("b-low", "d1", "1.2.0")
    service.report_progress(low_t.task_id, UpgradeStatus.FAILED)

    service.create_batch("b-high", to_version="3.0.0", min_from_version="1.0.0",
                         max_failure_ratio=1.0, priority=10)
    service.create_batch("b-mid", to_version="2.1.0", min_from_version="1.0.0",
                         max_failure_ratio=1.0, priority=0)

    # 低优先级不能从属主（哪怕其任务已终态）手里抢设备
    with pytest.raises(UpgradeError, match="cannot preempt"):
        service.dispatch("b-mid", "d1", "1.2.0")
    # 被挡下不留中间状态：没有任务产生，b-mid 仍可下发别的设备
    assert service._storage.task_for_batch_device("b-mid", "d1") is None
    other = service.dispatch("b-mid", "d9", "1.2.0")

    # 高优先级可以从 RUNNING 的低优属主手里抢占名额；抢占后属主转移，
    # 原属主 b-low 不再阻挡其他批次
    high_t = service.dispatch("b-high", "d1", "1.2.0")
    inflight = service.dispatch("b-high", "d8", "1.2.0")  # 第二台，暂停时仍在途
    assert service._holder_of("d1").batch_id == "b-high"
    # 高优任务先跑到终态（释放进行中互斥），只留名额归属
    service.report_progress(high_t.task_id, UpgradeStatus.DOWNLOADING)
    service.report_progress(high_t.task_id, UpgradeStatus.FLASHING)
    service.report_progress(high_t.task_id, UpgradeStatus.SUCCESS)
    # 中优依旧不能从新属主（高优）手里抢
    with pytest.raises(UpgradeError, match="cannot preempt"):
        service.dispatch("b-mid", "d1", "1.2.0")

    # 高优批次暂停：停止新下发；d8 的在途任务允许自然结束
    service.pause_batch("b-high")
    service.report_progress(inflight.task_id, UpgradeStatus.DOWNLOADING)
    service.report_progress(inflight.task_id, UpgradeStatus.FLASHING)
    service.report_progress(inflight.task_id, UpgradeStatus.SUCCESS)
    assert service._holder_of("d1").batch_id == "b-low"  # b-low 仍 RUNNING，顺延回它
    # b-low 也暂停（释放剩余占用）后，名额才完全空出
    service.pause_batch("b-low")
    assert service._holder_of("d1") is None
    mid_t = service.dispatch("b-mid", "d1", "1.2.0")

    # b-mid 占用 d1 后再下发一台 d2（其名下任务）。b-high 恢复：
    service.resume_batch("b-high")
    with pytest.raises(UpgradeError, match="already dispatched"):
        service.dispatch("b-high", "d1", "1.2.0")  # 自己下发过的 d1，永不重发
    service.dispatch("b-mid", "d2", "1.2.0")
    # d1 仍在 b-mid 手里；被挡下来的 b-mid 永远无法主动从高优 b-high 抢设备
    with pytest.raises(UpgradeError, match="cannot preempt"):
        service.dispatch("b-mid", "d8", "1.2.0")  # d8 已属 b-high，b-mid 抢不走
    print(f"[判定依据] 名额链：b-low 占 d1 -> b-mid 抢失败(无残留，d9 任务 {other.task_id[:8]}) "
          f"-> b-high 抢占({high_t.task_id[:8]}) -> b-mid 仍被挡 -> b-high 暂停、"
          f"在途任务自然 success -> b-low 暂停释放 -> b-mid 占用({mid_t.task_id[:8]}) "
          f"-> b-high 恢复后仍不可抢回")


def test_circuit_breaker_releases_device_capacity_for_other_batch(svc):
    service, now = svc
    service.create_batch("b-a", to_version="2.0.0", min_from_version="1.0.0",
                         max_failure_ratio=0.5)
    t1 = service.dispatch("b-a", "d1", "1.2.0")
    t2 = service.dispatch("b-a", "d2", "1.2.0")
    service.report_progress(t1.task_id, UpgradeStatus.FAILED)
    service.report_progress(t2.task_id, UpgradeStatus.FAILED)  # 失败率 2/2 -> 熔断
    batch_a = service._storage.load_batch("b-a")
    assert batch_a.pause_reason is PauseReason.CIRCUIT_BREAKER

    # 即使其他批次优先级更低，熔断后名额已释放，可以占用 d1
    service.create_batch("b-b", to_version="2.0.0", min_from_version="1.0.0",
                         max_failure_ratio=1.0, priority=-1)
    tb = service.dispatch("b-b", "d1", "1.2.0")
    print(f"[判定依据] b-a 熔断后 d1 名额释放，低优先级 b-b 下发成功 {tb.task_id[:8]}")


def test_same_priority_first_created_wins(svc):
    service, _ = svc
    service.create_batch("bx1", to_version="2.0.0", min_from_version="1.0.0",
                         max_failure_ratio=1.0, priority=5)
    service.create_batch("bx2", to_version="2.0.0", min_from_version="1.0.0",
                         max_failure_ratio=1.0, priority=5)
    tx = service.dispatch("bx1", "dx", "1.0.0")
    service.report_progress(tx.task_id, UpgradeStatus.FAILED)  # 终态后只剩名额归属问题
    with pytest.raises(UpgradeError, match="cannot preempt"):
        service.dispatch("bx2", "dx", "1.0.0")  # 同优先级，先创建者占用
    print("[判定依据] 同优先级下 bx1 先创建占用 dx，bx2 不可抢占")


def test_high_priority_preempts_inflight_task_dispatched_by_low_priority(svc):
    service, _ = svc
    # 低优先创建先下发，任务仍在途（downloading）——旧实现在此把后到的高优
    # 请求以 "already has active task" 直接挡回，优先级完全失效
    service.create_batch("b-low", to_version="2.0.0", min_from_version="1.0.0",
                         max_failure_ratio=0.0, priority=1)
    low_t = service.dispatch("b-low", "d1", "1.2.0")
    service.report_progress(low_t.task_id, UpgradeStatus.DOWNLOADING)

    service.create_batch("b-high", to_version="3.0.0", min_from_version="1.0.0",
                         max_failure_ratio=1.0, priority=10)
    # 高优后到：不必等低优任务跑完或超时，立即抢到设备
    high_t = service.dispatch("b-high", "d1", "1.2.0")
    assert high_t.task_id != low_t.task_id

    # 低优在途任务被干净置为 cancelled（终态），设备上唯一进行中任务属于高优
    old = service._storage.task_for_batch_device("b-low", "d1")
    assert old.status is UpgradeStatus.CANCELLED
    assert service._storage.active_task_for_device("d1").task_id == high_t.task_id
    assert service._holder_of("d1").batch_id == "b-high"

    # 被抢占任务此后再回报一律被终态规则拒绝，不会把设备状态写回高优任务
    with pytest.raises(UpgradeError, match="terminal"):
        service.report_progress(low_t.task_id, UpgradeStatus.FLASHING)
    # 抢占取消不计失败：max_failure_ratio=0.0 的低优批次没有被熔断
    assert service._storage.load_batch("b-low").status is BatchStatus.RUNNING
    print(f"[判定依据] 低优在途任务 {low_t.task_id[:8]} 被后到高优任务 "
          f"{high_t.task_id[:8]} 抢占：旧任务 cancelled 且不计失败，"
          f"d1 唯一在途任务属于 b-high")


def test_low_priority_cannot_take_back_after_high_priority_takes_over(svc):
    service, _ = svc
    service.create_batch("b-low", to_version="2.0.0", min_from_version="1.0.0",
                         max_failure_ratio=1.0, priority=1)
    low_t = service.dispatch("b-low", "d1", "1.2.0")

    service.create_batch("b-high", to_version="3.0.0", min_from_version="1.0.0",
                         max_failure_ratio=1.0, priority=10)
    high_t = service.dispatch("b-high", "d1", "1.2.0")  # 抢占，低优任务 cancelled
    service.report_progress(high_t.task_id, UpgradeStatus.DOWNLOADING)

    # 低优批次无论继续推进还是暂停恢复后再来，都不能把 d1 抢回：
    # 其名下已有该设备的 cancelled 记录，同批次设备唯一约束先挡下
    with pytest.raises(UpgradeError, match="already dispatched"):
        service.dispatch("b-low", "d1", "1.2.0")
    service.pause_batch("b-low")
    service.resume_batch("b-low")
    with pytest.raises(UpgradeError, match="already dispatched"):
        service.dispatch("b-low", "d1", "1.2.0")
    # 高优任务正常在跑，同一台设备从未被下发第二次（每批次一行）
    assert service._storage.active_task_for_device("d1").task_id == high_t.task_id
    low_rows = [t for t in service._storage.load_tasks("b-low") if t.device_id == "d1"]
    assert len(low_rows) == 1 and low_rows[0].task_id == low_t.task_id
    print("[判定依据] 高优接管后，低优继续推进与暂停恢复后重发都被 already "
          "dispatched 挡回；d1 在每个批次各只有一行，未被下发两次")


def test_rejected_batch_leaves_no_half_state(svc):
    service, _ = svc
    service.create_batch("b-low", to_version="2.0.0", min_from_version="1.0.0",
                         max_failure_ratio=1.0, priority=1)
    service.dispatch("b-low", "d1", "1.2.0")
    service.create_batch("b-high", to_version="3.0.0", min_from_version="1.0.0",
                         max_failure_ratio=1.0, priority=10)
    service.dispatch("b-high", "d1", "1.2.0")  # 高优占住 d1
    service.create_batch("b-mid", to_version="2.5.0", min_from_version="1.0.0",
                         max_failure_ratio=1.0, priority=5)

    with pytest.raises(UpgradeError, match="cannot preempt"):
        service.dispatch("b-mid", "d1", "1.2.0")
    # 被挡下：b-mid 名下没有 d1 的任何记录（连 cancelled 行都没有），
    # d1 的在途任务状态完全不受影响，b-mid 仍可下发其他设备
    assert service._storage.task_for_batch_device("b-mid", "d1") is None
    active = service._storage.active_task_for_device("d1")
    assert active.batch_id == "b-high" and active.status is UpgradeStatus.PENDING
    other = service.dispatch("b-mid", "d2", "1.2.0")
    print(f"[判定依据] b-mid 抢 d1 被挡后无任务行、d1 在途任务不受影响；"
          f"b-mid 对其他设备 d2 正常下发 {other.task_id[:8]}")


def test_paused_batch_inflight_task_is_released_immediately_on_contention(svc):
    service, _ = svc
    service.create_batch("b-low", to_version="2.0.0", min_from_version="1.0.0",
                         max_failure_ratio=1.0, priority=1)
    low_t = service.dispatch("b-low", "d1", "1.2.0")
    service.report_progress(low_t.task_id, UpgradeStatus.DOWNLOADING)
    # 低优批次暂停（手动）：不再参与属主排名；在途任务无人争抢时可自然结束，
    # 但其他 RUNNING 批次一来抢，立即释放，不必等超时
    service.pause_batch("b-low")
    assert service._holder_of("d1") is None

    service.create_batch("b-mid", to_version="2.5.0", min_from_version="1.0.0",
                         max_failure_ratio=1.0, priority=5)
    mid_t = service.dispatch("b-mid", "d1", "1.2.0")
    old = service._storage.task_for_batch_device("b-low", "d1")
    assert old.status is UpgradeStatus.CANCELLED
    assert service._storage.active_task_for_device("d1").task_id == mid_t.task_id
    print(f"[判定依据] b-low 暂停后 d1 名额立即空出，b-mid 下发即接管 "
          f"{mid_t.task_id[:8]}，原在途任务 {low_t.task_id[:8]} cancelled")


def test_circuit_breaker_inflight_task_is_released_immediately_on_contention(svc):
    service, now = svc
    service.create_batch("b-a", to_version="2.0.0", min_from_version="1.0.0",
                         max_failure_ratio=0.5)
    t1 = service.dispatch("b-a", "d1", "1.2.0")
    t2 = service.dispatch("b-a", "d2", "1.2.0")
    t3 = service.dispatch("b-a", "d3", "1.2.0")
    service.report_progress(t1.task_id, UpgradeStatus.DOWNLOADING)  # d1 在途
    service.report_progress(t2.task_id, UpgradeStatus.FAILED)
    service.report_progress(t3.task_id, UpgradeStatus.FAILED)  # 2/3 失败 -> 熔断
    batch_a = service._storage.load_batch("b-a")
    assert batch_a.status is BatchStatus.PAUSED
    assert batch_a.pause_reason is PauseReason.CIRCUIT_BREAKER
    # 熔断瞬间 d1 名额即退出属主排名，其他 RUNNING 批次可立即接管在途设备
    assert service._holder_of("d1") is None
    service.create_batch("b-b", to_version="3.0.0", min_from_version="1.0.0",
                         max_failure_ratio=1.0, priority=0)
    tb = service.dispatch("b-b", "d1", "1.2.0")
    assert service._storage.active_task_for_device("d1").task_id == tb.task_id
    assert service._storage.task_for_batch_device("b-a", "d1").status is UpgradeStatus.CANCELLED
    # 熔断批次在途任务被取消也不算失败（否则会在熔断状态上重复统计）
    print(f"[判定依据] b-a 熔断后 d1（在途）名额立即释放，b-b 接管 "
          f"{tb.task_id[:8]}，原在途任务 cancelled 且不重复计失败")


def test_concurrent_cross_batch_dispatch_single_winner_no_half_state(svc):
    service, _ = svc
    # 3 个不同优先级批次，12 线程并发抢同一台全新设备
    for name, prio in (("c-low", -1), ("c-mid", 5), ("c-high", 10)):
        service.create_batch(name, to_version="2.0.0", min_from_version="1.0.0",
                             max_failure_ratio=1.0, priority=prio)
    results, errors = [], []
    barrier = threading.Barrier(12)
    names = ("c-low", "c-mid", "c-high")

    def worker(batch):
        barrier.wait()
        try:
            results.append(service.dispatch(batch, "shared", "1.0.0"))
        except UpgradeError:
            errors.append(1)

    threads = []
    for i in range(12):
        threads.append(threading.Thread(target=worker, args=(names[i % 3],)))
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # 无论线程实际调度顺序如何（高优可能先到也可能后到），收敛结果必须确定：
    # 1) 设备全局只有一个进行中任务，且属主一定是最高优先级批次；
    # 2) 每个 (批次,设备) 至多一行，被抢占方只允许留下干净的 cancelled 终态行
    #    （不是半推半就的中间状态），纯被挡下的批次则一行都没有；
    # 3) 高优批次自己的重复下发被同批次唯一约束挡回，不会出现两行。
    all_tasks = [t for t in service._storage.load_tasks() if t.device_id == "shared"]
    active = [t for t in all_tasks if t.status not in TERMINAL_UPGRADE_STATES]
    assert len(active) == 1
    assert active[0].batch_id == "c-high"
    assert {t.batch_id for t in all_tasks} <= {"c-low", "c-mid", "c-high"}
    assert all(
        t.status is UpgradeStatus.CANCELLED
        for t in all_tasks if t.batch_id != "c-high"
    )
    high_rows = [t for t in all_tasks if t.batch_id == "c-high"]
    assert len(high_rows) == 1
    print(f"[判定依据] 12 线程跨 3 优先级批次并发抢 shared：成功={len(results)} "
          f"拒绝={len(errors)}，任务行={len(all_tasks)}；唯一在途任务属 c-high，"
          f"其余批次仅有 cancelled 终态行或无记录，无中间残留")
