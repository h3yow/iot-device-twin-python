"""升级：重复下发约束、中断（失败/超时）识别、批次熔断与版本门限。"""
import threading

import pytest

from app.models import BatchStatus, UpgradeStatus
from app.storage import Storage
from app.upgrade import UpgradeError, UpgradeService


@pytest.fixture()
def svc(tmp_path):
    storage = Storage(str(tmp_path / "up.db"))
    now = [1000.0]
    service = UpgradeService(storage, clock=lambda: now[0])
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


def test_interrupted_upgrade_times_out_and_device_can_redispatch(svc):
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

    # 超时为终态：同一批次内不允许再次向该设备下发（失败设备不重复下发）
    with pytest.raises(UpgradeError, match="already has task"):
        service.dispatch("b-retry", "d1", "1.2.0")

    # 修复后的重试只能通过新批次发起（旧任务已终态，容量已释放）
    service.create_batch("b-retry-2", to_version="2.0.1", min_from_version="1.0.0",
                         max_failure_ratio=1.0)
    t2 = service.dispatch("b-retry-2", "d1", "1.2.0")
    assert t2.task_id != t.task_id
    print(f"[判定依据] 超时设备 d1 不再进旧批次，由新批次 b-retry-2 重新下发 "
          f"task={t2.task_id[:8]}")


def test_terminal_task_rejects_further_progress(svc):
    service, _ = svc
    t = service.dispatch("b1", "d1", "1.2.0")
    service.report_progress(t.task_id, UpgradeStatus.FAILED)
    with pytest.raises(UpgradeError, match="terminal"):
        service.report_progress(t.task_id, UpgradeStatus.DOWNLOADING)
    print(f"[判定依据] 终态任务 {t.task_id[:8]} 拒绝再次迁移，状态保持 failed")
