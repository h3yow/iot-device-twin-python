"""批次运维控制：暂停/恢复、失败熔断原因、优先级容量约束、失败设备不重复下发。"""
import threading

import pytest

from app.models import BatchStatus, DeviceSlotStatus, PauseReason, UpgradeStatus
from app.storage import Storage
from app.upgrade import UpgradeError, UpgradeService


@pytest.fixture()
def svc(tmp_path):
    storage = Storage(str(tmp_path / "batch.db"))
    now = [1000.0]
    service = UpgradeService(storage, clock=lambda: now[0])
    yield service, storage, now
    storage.close()


def _succeed(service, task):
    service.report_progress(task.task_id, UpgradeStatus.DOWNLOADING)
    service.report_progress(task.task_id, UpgradeStatus.FLASHING)
    return service.report_progress(task.task_id, UpgradeStatus.SUCCESS)


def _make_batch(service, bid="b1", ratio=0.5, priority=0):
    return service.create_batch(
        bid, to_version="2.0.0", min_from_version="1.0.0",
        max_failure_ratio=ratio, priority=priority,
    )


# ---------- 暂停 / 恢复 ----------

def test_pause_stops_new_dispatch_but_inflight_continues(svc):
    service, _, _ = svc
    _make_batch(service)
    t1 = service.dispatch("b1", "d1", "1.2.0")
    service.report_progress(t1.task_id, UpgradeStatus.DOWNLOADING)

    batch = service.pause_batch("b1")
    assert batch.status is BatchStatus.PAUSED
    assert batch.pause_reason is PauseReason.MANUAL
    # 暂停后不能再向新设备下发
    with pytest.raises(UpgradeError, match="dispatch stopped"):
        service.dispatch("b1", "d2", "1.2.0")
    with pytest.raises(UpgradeError, match="dispatch stopped"):
        service.enroll("b1", "d2")
    # 已经在跑的设备允许自然结束
    service.report_progress(t1.task_id, UpgradeStatus.FLASHING)
    t1 = service.report_progress(t1.task_id, UpgradeStatus.SUCCESS)
    assert t1.status is UpgradeStatus.SUCCESS
    print("[判定依据] 暂停后 d2 下发/enroll 被拒，进行中 d1 正常走到 success")


def test_pause_resume_continues_without_repull_or_duplicate(svc):
    service, _, _ = svc
    _make_batch(service)
    t1 = service.dispatch("b1", "d1", "1.2.0")
    service.enroll("b1", "d2")  # 已占名额但尚未下发
    service.pause_batch("b1")
    _succeed(service, t1)

    batch = service.resume_batch("b1")
    assert batch.status is BatchStatus.RUNNING
    assert batch.pause_reason is None
    # d1 已成功：不会被再次下发
    with pytest.raises(UpgradeError, match="already has task"):
        service.dispatch("b1", "d1", "1.2.0")
    # d2 名额仍在手里，接着原进度继续，只下发一次
    t2 = service.dispatch("b1", "d2", "1.2.0")
    service.report_progress(t2.task_id, UpgradeStatus.FAILED)
    with pytest.raises(UpgradeError, match="already has task"):
        service.dispatch("b1", "d2", "1.2.0")
    tasks = [t for t in service._storage.load_tasks("b1") if t.device_id == "d2"]
    assert len(tasks) == 1
    print("[判定依据] 恢复后已完成 d1 不重拉、未下发 d2 继续、d2 失败后不再下发")


def test_double_pause_and_resume_guard(svc):
    service, _, _ = svc
    _make_batch(service)
    service.pause_batch("b1")
    with pytest.raises(UpgradeError, match="cannot pause"):
        service.pause_batch("b1")
    service.resume_batch("b1")
    with pytest.raises(UpgradeError, match="cannot resume"):
        service.resume_batch("b1")
    print("[判定依据] 重复暂停/恢复被状态机拒绝")


# ---------- 熔断 ----------

def test_circuit_breaker_marks_circuit_reason(svc):
    service, _, _ = svc
    _make_batch(service, ratio=0.5)
    t1 = service.dispatch("b1", "d1", "1.2.0")
    t2 = service.dispatch("b1", "d2", "1.2.0")
    t3 = service.dispatch("b1", "d3", "1.2.0")
    _succeed(service, t1)
    service.report_progress(t2.task_id, UpgradeStatus.FAILED)
    service.report_progress(t3.task_id, UpgradeStatus.FAILED)  # 2/3 > 0.5

    batch = service._storage.load_batch("b1")
    assert batch.status is BatchStatus.PAUSED
    assert batch.pause_reason is PauseReason.CIRCUIT
    print("[判定依据] 失败率 2/3 > 0.5 -> PAUSED 且 pause_reason=circuit")


def test_timeout_counts_toward_circuit(svc):
    service, _, now = svc
    _make_batch(service, ratio=0.5)
    t1 = service.dispatch("b1", "d1", "1.2.0")
    t2 = service.dispatch("b1", "d2", "1.2.0")
    now[0] += 4000.0
    service.check_timeouts(now=now[0], timeout_seconds=3600)
    batch = service._storage.load_batch("b1")
    assert batch.status is BatchStatus.PAUSED
    assert batch.pause_reason is PauseReason.CIRCUIT
    # 熔断后新设备进不来
    with pytest.raises(UpgradeError, match="dispatch stopped"):
        service.dispatch("b1", "d3", "1.2.0")
    # 超时设备不会因为重试推进而再次收到任务：即使运维恢复批次也一样
    service.resume_batch("b1")
    with pytest.raises(UpgradeError, match="already has task"):
        service.dispatch("b1", "d1", "1.2.0")
    with pytest.raises(UpgradeError, match="already has task"):
        service.dispatch("b1", "d2", "1.2.0")
    print("[判定依据] 超时计入熔断；熔断后新设备停止推进，恢复后超时设备仍不重发")


def test_failed_device_never_redispatched_even_after_resume(svc):
    service, _, _ = svc
    _make_batch(service, ratio=1.0)  # 不会自动熔断，手动暂停再恢复
    t1 = service.dispatch("b1", "d1", "1.2.0")
    service.report_progress(t1.task_id, UpgradeStatus.FAILED)
    service.pause_batch("b1")
    service.resume_batch("b1")
    with pytest.raises(UpgradeError, match="already has task"):
        service.dispatch("b1", "d1", "1.2.0")
    print("[判定依据] failed 设备在暂停-恢复循环后仍收不到第二次任务")


# ---------- 优先级 / 容量约束 ----------

def test_same_device_only_in_one_running_batch(svc):
    service, _, _ = svc
    _make_batch(service, "low", ratio=1.0, priority=1)
    _make_batch(service, "high", ratio=1.0, priority=1)  # 同优先级
    service.enroll("low", "d1")
    with pytest.raises(UpgradeError, match="occupied by batch low"):
        service.enroll("high", "d1")
    with pytest.raises(UpgradeError, match="occupied by batch low"):
        service.dispatch("high", "d1", "1.2.0")
    print("[判定依据] 同优先级先到先得，后到批次不能抢，也没有部分名额残留")


def test_higher_priority_takes_over_unused_slot(svc):
    service, storage, _ = svc
    _make_batch(service, "low", ratio=1.0, priority=1)
    _make_batch(service, "high", ratio=1.0, priority=10)
    service.enroll("low", "d1")  # low 占名额但尚未下发
    service.enroll("high", "d1")  # 高优先级接管

    assert storage.reserved_slot("high", "d1") is not None
    low_entry = storage.load_batch_devices("low")[0]
    assert low_entry.slot is DeviceSlotStatus.DISPLACED
    # 接管后 high 可以正常下发；low 恢复推进时也拿不回这个设备
    t = service.dispatch("high", "d1", "1.2.0")
    assert t.batch_id == "high"
    with pytest.raises(UpgradeError, match="in-flight task"):
        service.dispatch("low", "d1", "1.2.0")
    print("[判定依据] high 接管 low 未使用名额（low 记为 displaced），随后独占下发")


def test_inflight_slot_cannot_be_stolen_by_any_priority(svc):
    service, _, _ = svc
    _make_batch(service, "low", ratio=1.0, priority=1)
    _make_batch(service, "high", ratio=1.0, priority=100)
    t = service.dispatch("low", "d1", "1.2.0")
    service.report_progress(t.task_id, UpgradeStatus.DOWNLOADING)
    # 即便高优先级，也不能抢正在升级的设备
    with pytest.raises(UpgradeError, match="in-flight task"):
        service.enroll("high", "d1")
    # low 的名额仍然有效，没有中间态
    assert service._storage.reserved_slot("low", "d1") is not None
    print("[判定依据] downloading 中的设备名额不可被任何优先级抢占")


def test_paused_batch_releases_slots_for_others(svc):
    service, storage, _ = svc
    _make_batch(service, "b1", ratio=1.0, priority=1)
    _make_batch(service, "b2", ratio=1.0, priority=1)
    service.enroll("b1", "d1")
    service.pause_batch("b1")
    # b1 暂停，名额释放：同优先级的 b2 可以拿走（b1 记录变 displaced）
    service.enroll("b2", "d1")
    assert storage.reserved_slot("b2", "d1") is not None
    assert storage.load_batch_devices("b1")[0].slot is DeviceSlotStatus.DISPLACED
    # b1 恢复推进，d1 不会被重新拉进 b1（即使 b2 还没下发）
    service.resume_batch("b1")
    with pytest.raises(UpgradeError, match="occupied by batch b2"):
        service.enroll("b1", "d1")
    print("[判定依据] 暂停释放名额给 b2；恢复后 b1 不把 d1 抢回来")


def test_circuit_paused_batch_releases_slots(svc):
    service, _, _ = svc
    _make_batch(service, "b1", ratio=0.0)  # 只要有 1 个失败就熔断
    _make_batch(service, "b2", ratio=1.0, priority=1)
    t1 = service.dispatch("b1", "d1", "1.2.0")
    service.enroll("b1", "d2")  # 尚未下发的名额
    service.report_progress(t1.task_id, UpgradeStatus.FAILED)
    # 1/1 失败（门限 0.0，严格大于即熔断）
    batch = service._storage.load_batch("b1")
    assert batch.pause_reason is PauseReason.CIRCUIT
    # d2 的空名额被熔断释放，b2 可占用
    service.enroll("b2", "d2")
    t2 = service.dispatch("b2", "d2", "1.2.0")
    assert t2.batch_id == "b2"
    print("[判定依据] 熔断后未使用名额 d2 释放给 b2 使用")


def test_concurrent_enroll_single_occupier(svc):
    service, _, _ = svc
    _make_batch(service, "a", ratio=1.0, priority=1)
    _make_batch(service, "b", ratio=1.0, priority=1)
    outcomes = []
    barrier = threading.Barrier(20)

    def worker(bid):
        barrier.wait()
        try:
            service.enroll(bid, "d1")
            outcomes.append(bid)
        except UpgradeError:
            outcomes.append("rejected")

    threads = [
        threading.Thread(target=worker, args=("a" if i % 2 == 0 else "b",))
        for i in range(20)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    winners = {o for o in outcomes if o != "rejected"}
    assert len(winners) == 1
    print(f"[判定依据] 20 线程并发 enroll 两个批次：唯一占用方={winners}")
