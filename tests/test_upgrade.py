"""升级：重复下发约束、中断（失败/超时）识别、批次熔断与版本门限。"""
import threading

import pytest

from app.models import BatchStatus, PauseReason, UpgradeStatus
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


def test_concurrent_cross_batch_dispatch_single_winner_no_half_state(svc):
    service, _ = svc
    # 3 个同优先级批次，12 线程并发抢同一台全新设备
    for name in ("c1", "c2", "c3"):
        service.create_batch(name, to_version="2.0.0", min_from_version="1.0.0",
                             max_failure_ratio=1.0, priority=1)
    results, errors = [], []
    barrier = threading.Barrier(12)

    def worker(batch):
        barrier.wait()
        try:
            results.append(service.dispatch(batch, "shared", "1.0.0"))
        except UpgradeError:
            errors.append(1)

    threads = []
    for i in range(12):
        threads.append(threading.Thread(target=worker, args=(f"c{i % 3 + 1}",)))
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(results) == 1 and len(errors) == 11
    # 设备名下全局只有一个进行中任务，没有任何半推半就的残留
    all_tasks = [t for t in service._storage.load_tasks() if t.device_id == "shared"]
    assert len(all_tasks) == 1
    assert service._storage.active_task_for_device("shared") is not None
    print(f"[判定依据] 12 线程跨 3 批次并发抢 shared：成功={len(results)} "
          f"拒绝={len(errors)}，任务总数={len(all_tasks)}（无中间残留）")


def test_high_priority_preempts_low_priority_inflight_task(svc):
    """低优先级先下发（任务在途），高优先级后到：直接抢占，不等对方跑完。"""
    service, _ = svc
    service.create_batch("b-low", to_version="2.0.0", min_from_version="1.0.0",
                         max_failure_ratio=1.0, priority=1)
    low_t = service.dispatch("b-low", "d1", "1.2.0")
    service.report_progress(low_t.task_id, UpgradeStatus.DOWNLOADING)  # 在途

    service.create_batch("b-high", to_version="3.0.0", min_from_version="1.0.0",
                         max_failure_ratio=1.0, priority=10)
    high_t = service.dispatch("b-high", "d1", "1.2.0")  # 抢占，不再被挡

    # 低优批次的在途任务被置为 CANCELLED 终态；设备上只剩高优一个进行中任务
    low_task = service._storage.task_for_batch_device("b-low", "d1")
    assert low_task.status is UpgradeStatus.CANCELLED
    active = service._storage.active_tasks_for_device("d1")
    assert [t.task_id for t in active] == [high_t.task_id]

    # 低优批次抢不回来：迟到回报被拒、重发被拒、暂停/恢复后依旧被拒
    with pytest.raises(UpgradeError, match="terminal"):
        service.report_progress(low_t.task_id, UpgradeStatus.FLASHING)
    with pytest.raises(UpgradeError, match="already dispatched"):
        service.dispatch("b-low", "d1", "1.2.0")
    service.pause_batch("b-low")
    service.resume_batch("b-low")
    with pytest.raises(UpgradeError, match="already dispatched"):
        service.dispatch("b-low", "d1", "1.2.0")

    # 高优任务正常推进到成功，全程设备没有被下发两次
    service.report_progress(high_t.task_id, UpgradeStatus.DOWNLOADING)
    service.report_progress(high_t.task_id, UpgradeStatus.FLASHING)
    service.report_progress(high_t.task_id, UpgradeStatus.SUCCESS)
    d1_tasks = [t for t in service._storage.load_tasks() if t.device_id == "d1"]
    assert len(d1_tasks) == 2  # low(CANCELLED) + high(SUCCESS)，无第三次下发
    print(f"[判定依据] b-low(p1) 在途任务 {low_t.task_id[:8]} 被 b-high(p10) "
          f"抢占为 cancelled；高优任务 {high_t.task_id[:8]} 跑到 success；"
          f"b-low 恢复后仍抢不回（already dispatched），d1 任务总数={len(d1_tasks)}")


def test_equal_priority_inflight_task_not_preempted(svc):
    """同优先级：在途任务先占先得，早创建的批次也不能抢，被挡下无残留。"""
    service, _ = svc
    service.create_batch("bx1", to_version="2.0.0", min_from_version="1.0.0",
                         max_failure_ratio=1.0, priority=5)  # 先创建
    service.create_batch("bx2", to_version="2.0.0", min_from_version="1.0.0",
                         max_failure_ratio=1.0, priority=5)
    t2 = service.dispatch("bx2", "d1", "1.2.0")  # bx2 先下发，任务在途

    with pytest.raises(UpgradeError, match="cannot preempt inflight"):
        service.dispatch("bx1", "d1", "1.2.0")
    # 被挡下不留任何半截记录
    assert service._storage.task_for_batch_device("bx1", "d1") is None
    assert [t.task_id for t in service._storage.active_tasks_for_device("d1")] == [t2.task_id]

    # bx2 在途任务终态后只剩名额归属：bx1 创建更早，排名更高，可以接管
    service.report_progress(t2.task_id, UpgradeStatus.FAILED)
    t1 = service.dispatch("bx1", "d1", "1.2.0")
    print(f"[判定依据] bx2 在途时 bx1 被挡（无残留）；bx2 任务终态后 bx1 凭"
          f"创建更早接管 d1，任务 {t1.task_id[:8]}")


def test_paused_batch_releases_device_even_with_inflight_task(svc):
    """批次暂停后名额立即释放：其在途任务不把设备挂住，其他批次可接管。"""
    service, _ = svc
    service.create_batch("b-p", to_version="2.0.0", min_from_version="1.0.0",
                         max_failure_ratio=1.0, priority=5)
    tp = service.dispatch("b-p", "d1", "1.2.0")
    service.report_progress(tp.task_id, UpgradeStatus.DOWNLOADING)  # 在途
    service.pause_batch("b-p")

    # 即使优先级更低，也能立即占用 d1：b-p 的在途任务被抢占取消
    service.create_batch("b-n", to_version="2.0.0", min_from_version="1.0.0",
                         max_failure_ratio=1.0, priority=0)
    tn = service.dispatch("b-n", "d1", "1.2.0")
    assert service._storage.task_for_batch_device("b-p", "d1").status is \
        UpgradeStatus.CANCELLED

    # b-p 恢复后也抢不回：d1 在它名下已是终态，永不重发
    service.resume_batch("b-p")
    with pytest.raises(UpgradeError, match="already dispatched"):
        service.dispatch("b-p", "d1", "1.2.0")
    assert service._holder_of("d1").batch_id == "b-n"
    print(f"[判定依据] b-p 暂停后在途任务 {tp.task_id[:8]} 被低优先级 b-n "
          f"抢占取消（任务 {tn.task_id[:8]}），设备不挂住；恢复后仍抢不回")


def test_circuit_breaker_releases_inflight_device_immediately(svc):
    """熔断暂停的批次，其在途任务遗留的设备名额立即释放给其他批次。"""
    service, _ = svc
    service.create_batch("b-a", to_version="2.0.0", min_from_version="1.0.0",
                         max_failure_ratio=0.5)
    t1 = service.dispatch("b-a", "d1", "1.2.0")
    t2 = service.dispatch("b-a", "d2", "1.2.0")
    t3 = service.dispatch("b-a", "d3", "1.2.0")  # 熔断时仍在途
    service.report_progress(t1.task_id, UpgradeStatus.FAILED)
    service.report_progress(t2.task_id, UpgradeStatus.FAILED)  # 2/3 > 0.5 -> 熔断
    batch_a = service._storage.load_batch("b-a")
    assert batch_a.status is BatchStatus.PAUSED
    assert batch_a.pause_reason is PauseReason.CIRCUIT_BREAKER

    service.create_batch("b-b", to_version="2.0.0", min_from_version="1.0.0",
                         max_failure_ratio=1.0, priority=-1)
    tb = service.dispatch("b-b", "d3", "1.2.0")  # 在途设备名额立即释放
    assert service._storage.task_for_batch_device("b-a", "d3").status is \
        UpgradeStatus.CANCELLED
    print(f"[判定依据] b-a 熔断(circuit_breaker)后 d3 在途任务 {t3.task_id[:8]} "
          f"被抢占取消，b-b 立即下发成功 {tb.task_id[:8]}")


def test_concurrent_high_low_priority_contention_high_always_wins(svc):
    """高低优先级并发抢同一台设备：无论谁先抢到锁，最终在途任务都属于高优。"""
    service, _ = svc
    service.create_batch("b-low", to_version="2.0.0", min_from_version="1.0.0",
                         max_failure_ratio=1.0, priority=1)
    service.create_batch("b-high", to_version="3.0.0", min_from_version="1.0.0",
                         max_failure_ratio=1.0, priority=10)
    results, errors = [], []
    barrier = threading.Barrier(12)

    def worker(batch):
        barrier.wait()
        try:
            results.append(service.dispatch(batch, "shared", "1.0.0"))
        except UpgradeError:
            errors.append(1)

    threads = [threading.Thread(target=worker, args=(b,))
               for b in ("b-low", "b-high") * 6]
    for t in threads: t.start()
    for t in threads: t.join()

    # 最终不变式：设备上至多一个进行中任务，且一定属于高优批次
    active = service._storage.active_tasks_for_device("shared")
    assert len(active) == 1 and active[0].batch_id == "b-high"
    # 低优批次名下的任务（若曾抢先创建）全部已被抢占为 CANCELLED 终态
    low_tasks = [t for t in service._storage.load_tasks("b-low")
                 if t.device_id == "shared"]
    assert all(t.status is UpgradeStatus.CANCELLED for t in low_tasks)
    # 高优接管后低优再试仍被挡，且不留新残留
    with pytest.raises(UpgradeError):
        service.dispatch("b-low", "shared", "1.0.0")
    assert len([t for t in service._storage.load_tasks("b-low")
                if t.device_id == "shared"]) == len(low_tasks)
    print(f"[判定依据] 12 线程高低优并发抢 shared：成功={len(results)} "
          f"拒绝={len(errors)}；最终在途任务属 b-high，"
          f"b-low 残留任务数={len(low_tasks)}（全部 cancelled 终态）")
