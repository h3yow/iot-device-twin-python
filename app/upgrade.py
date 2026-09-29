"""固件升级：分批下发、状态跟踪、失败熔断、暂停恢复与优先级容量约束。"""
from __future__ import annotations

import threading
import time
import uuid
from collections.abc import Callable

from app.models import (
    BatchDevice,
    BatchStatus,
    DeviceSlotStatus,
    PauseReason,
    TERMINAL_UPGRADE_STATES,
    UpgradeBatch,
    UpgradeStatus,
    UpgradeTask,
)
from app.storage import Storage


class UpgradeError(Exception):
    """升级约束被违反（版本门限/并发任务/批次状态/容量争抢/非法状态迁移）。"""


# 合法状态迁移：终态不可再迁移
_TRANSITIONS: dict[UpgradeStatus, frozenset[UpgradeStatus]] = {
    UpgradeStatus.PENDING: frozenset(
        {UpgradeStatus.DOWNLOADING, UpgradeStatus.FAILED, UpgradeStatus.CANCELLED}
    ),
    UpgradeStatus.DOWNLOADING: frozenset(
        {UpgradeStatus.FLASHING, UpgradeStatus.FAILED, UpgradeStatus.CANCELLED}
    ),
    UpgradeStatus.FLASHING: frozenset(
        {UpgradeStatus.SUCCESS, UpgradeStatus.FAILED, UpgradeStatus.CANCELLED}
    ),
}


def parse_version(v: str) -> tuple[int, ...]:
    try:
        return tuple(int(p) for p in v.split("."))
    except ValueError:
        raise UpgradeError(f"invalid version string: {v!r}") from None


class UpgradeService:
    """升级约束：

    - 同一设备同一时刻至多一个进行中（非终态）任务；
    - 设备当前版本必须 >= 批次 min_from_version（版本门限）；
    - 批次非 RUNNING（暂停/熔断/完成）时禁止继续下发；
    - 同批次内一台设备至多一个任务：失败/超时后不再向其下发；
    - 设备名额：同一时刻至多一个推进中的批次占用同一台设备；
      高优先级批次可接管低优先级批次尚未下发的名额，反之被挡；
    - 暂停/熔断后名额释放给其他批次；恢复时仅取回仍空闲的名额；
    - 失败(含超时)设备数 / 批次任务总数 > max_failure_ratio 时批次自动熔断暂停；
    - 任务状态机迁移非法时抛 UpgradeError。
    """

    def __init__(self, storage: Storage, clock: Callable[[], float] = time.time) -> None:
        self._storage = storage
        self._clock = clock
        self._lock = threading.Lock()

    def create_batch(
        self,
        batch_id: str,
        to_version: str,
        min_from_version: str,
        max_failure_ratio: float,
        priority: int = 0,
    ) -> UpgradeBatch:
        parse_version(to_version)
        parse_version(min_from_version)
        if not 0.0 <= max_failure_ratio <= 1.0:
            raise UpgradeError("max_failure_ratio must be in [0, 1]")
        if not isinstance(priority, int):
            raise UpgradeError("priority must be an integer")
        with self._lock:
            if self._storage.load_batch(batch_id) is not None:
                raise UpgradeError(f"batch already exists: {batch_id}")
            batch = UpgradeBatch(
                batch_id=batch_id,
                to_version=to_version,
                min_from_version=min_from_version,
                max_failure_ratio=max_failure_ratio,
                status=BatchStatus.RUNNING,
                priority=priority,
            )
            self._storage.save_batch(batch)
            return batch

    def enroll(self, batch_id: str, device_id: str) -> None:
        """把设备纳入批次目标清单并尝试占用名额。

        - 批次必须 RUNNING；
        - 设备已被其他推进中批次占用时：本批次优先级更高且对方尚未向该设备
          下发任务，才可接管；否则抛 UpgradeError，不留下中间状态；
        - 暂停/熔断/完成批次的名额视为已释放，可直接占用；
        - 重复 enroll 幂等：已是本批次有效名额时直接返回。
        """
        with self._lock:
            self._acquire_slot(batch_id, device_id)

    def dispatch(self, batch_id: str, device_id: str, from_version: str) -> UpgradeTask:
        """下发升级；违反门限/容量/批次状态约束时抛 UpgradeError。

        未显式 enroll 的设备会先原子地尝试获取名额；同一批次中已经存在该设备
        的任务（无论终态与否）都拒绝再次下发，保证失败/超时设备不被重发、
        暂停恢复也不会重复下发。
        """
        with self._lock:
            batch = self._storage.load_batch(batch_id)
            if batch is None:
                raise UpgradeError(f"unknown batch: {batch_id}")
            if batch.status != BatchStatus.RUNNING:
                raise UpgradeError(f"batch {batch_id} is {batch.status.value}, dispatch stopped")
            if parse_version(from_version) < parse_version(batch.min_from_version):
                raise UpgradeError(
                    f"device {device_id} version {from_version} below gate "
                    f"{batch.min_from_version}"
                )
            # 同批次内一台设备至多一个任务：终态任务同样挡住再次下发
            for t in self._storage.load_tasks(batch_id):
                if t.device_id == device_id:
                    raise UpgradeError(
                        f"device {device_id} already has task {t.task_id} "
                        f"({t.status.value}) in batch {batch_id}"
                    )
            self._acquire_slot(batch_id, device_id)
            # 兜底：进行中的任务在任意批次存在都不允许再下发
            active = self._storage.active_task_for_device(device_id)
            if active is not None:
                raise UpgradeError(
                    f"device {device_id} already has active task {active.task_id}"
                )
            task = UpgradeTask(
                task_id=uuid.uuid4().hex,
                device_id=device_id,
                batch_id=batch_id,
                from_version=from_version,
                to_version=batch.to_version,
                status=UpgradeStatus.PENDING,
                updated_at=self._clock(),
            )
            self._storage.save_task(task)
            return task

    def pause_batch(self, batch_id: str) -> UpgradeBatch:
        """手动暂停：停止向新设备继续下发，进行中任务允许自然结束。"""
        with self._lock:
            batch = self._storage.load_batch(batch_id)
            if batch is None:
                raise UpgradeError(f"unknown batch: {batch_id}")
            if batch.status != BatchStatus.RUNNING:
                raise UpgradeError(f"batch {batch_id} is {batch.status.value}, cannot pause")
            batch.status = BatchStatus.PAUSED
            batch.pause_reason = PauseReason.MANUAL
            self._storage.save_batch(batch)
            return batch

    def resume_batch(self, batch_id: str) -> UpgradeBatch:
        """恢复推进：仅继续从未下发的名额，已完成/失败/超时的设备不会被重拉。

        暂停期间被其他批次接管（且对方尚未下发）的名额保持 displaced，
        已在本批次下发过任务的设备即使名额空闲也不会被重复下发。
        """
        with self._lock:
            batch = self._storage.load_batch(batch_id)
            if batch is None:
                raise UpgradeError(f"unknown batch: {batch_id}")
            if batch.status != BatchStatus.PAUSED:
                raise UpgradeError(f"batch {batch_id} is {batch.status.value}, cannot resume")
            batch.status = BatchStatus.RUNNING
            batch.pause_reason = None
            self._storage.save_batch(batch)
            return batch

    def report_progress(self, task_id: str, status: UpgradeStatus) -> UpgradeTask:
        """设备回报升级进度；非法迁移抛 UpgradeError。"""
        with self._lock:
            task = self._find_task(task_id)
            if task.status in TERMINAL_UPGRADE_STATES:
                raise UpgradeError(f"task {task_id} already terminal: {task.status.value}")
            allowed = _TRANSITIONS.get(task.status, frozenset())
            if status not in allowed:
                raise UpgradeError(
                    f"illegal transition {task.status.value} -> {status.value}"
                )
            task.status = status
            task.updated_at = self._clock()
            self._storage.save_task(task)
            self._reevaluate_batch(task.batch_id)
            return task

    def check_timeouts(self, now: float, timeout_seconds: float) -> list[UpgradeTask]:
        """将进行中超时任务置为 TIMEOUT，返回被判定超时的任务。"""
        timed_out: list[UpgradeTask] = []
        with self._lock:
            for task in self._storage.load_tasks():
                if task.status in TERMINAL_UPGRADE_STATES:
                    continue
                if now - task.updated_at > timeout_seconds:
                    task.status = UpgradeStatus.TIMEOUT
                    task.updated_at = now
                    self._storage.save_task(task)
                    timed_out.append(task)
            for task in timed_out:
                self._reevaluate_batch(task.batch_id)
        return timed_out

    def close_batch(self, batch_id: str) -> UpgradeBatch:
        """显式收尾批次：仅当全部任务到达终态时才允许置为 COMPLETED。"""
        with self._lock:
            batch = self._storage.load_batch(batch_id)
            if batch is None:
                raise UpgradeError(f"unknown batch: {batch_id}")
            tasks = self._storage.load_tasks(batch_id)
            if any(t.status not in TERMINAL_UPGRADE_STATES for t in tasks):
                raise UpgradeError(f"batch {batch_id} still has active tasks")
            batch.status = BatchStatus.COMPLETED
            batch.pause_reason = None
            self._storage.save_batch(batch)
            return batch

    # ---- 内部 ----
    def _find_task(self, task_id: str) -> UpgradeTask:
        for t in self._storage.load_tasks():
            if t.task_id == task_id:
                return t
        raise UpgradeError(f"unknown task: {task_id}")

    def _acquire_slot(self, batch_id: str, device_id: str) -> None:
        """在已持锁前提下获取 (batch_id, device_id) 名额。

        判定规则（全部在同一把服务锁内完成，不存在中间态）：
        1. 设备存在任意批次的进行中任务 -> 硬占用，任何优先级都不能抢；
        2. RUNNING 批次持有尚未下发的 RESERVED 名额 -> 按优先级仲裁：
           优先级更高才能接管，优先级相同或更低被挡；
        3. 暂停/熔断/完成批次的名额，以及已下发任务且任务已终态的名额，
           视为已释放，可直接占用（原记录标记 displaced）。
        """
        batch = self._storage.load_batch(batch_id)
        if batch is None:
            raise UpgradeError(f"unknown batch: {batch_id}")
        if batch.status != BatchStatus.RUNNING:
            raise UpgradeError(f"batch {batch_id} is {batch.status.value}, dispatch stopped")
        if self._storage.reserved_slot(batch_id, device_id) is not None:
            return  # 幂等：名额已在自己手里

        # 进行中任务：硬占用，不可抢占
        active = self._storage.active_task_for_device(device_id)
        if active is not None:
            raise UpgradeError(
                f"device {device_id} has in-flight task {active.task_id} "
                f"of batch {active.batch_id}"
            )

        for occupant in self._storage.reserved_batches_for_device(device_id):
            if occupant == batch_id:
                continue
            entry = self._storage.reserved_slot(occupant, device_id)
            if entry is None:
                continue
            occ_batch = self._storage.load_batch(occupant)
            if occ_batch is None or occ_batch.status != BatchStatus.RUNNING:
                # 暂停/熔断/完成：名额已释放
                self._storage.save_batch_device(
                    BatchDevice(occupant, device_id, DeviceSlotStatus.DISPLACED)
                )
                continue
            used = any(
                t.device_id == device_id
                for t in self._storage.load_tasks(occupant)
            )
            if used:
                # 任务已终态（进行中已在上面硬挡）：名额随任务结束释放
                self._storage.save_batch_device(
                    BatchDevice(occupant, device_id, DeviceSlotStatus.DISPLACED)
                )
            elif occ_batch.priority >= batch.priority:
                raise UpgradeError(
                    f"device {device_id} occupied by batch {occupant} "
                    f"(priority {occ_batch.priority} >= {batch.priority})"
                )
            else:
                # 更高优先级：接管尚未使用的名额
                self._storage.save_batch_device(
                    BatchDevice(occupant, device_id, DeviceSlotStatus.DISPLACED)
                )
        self._storage.save_batch_device(
            BatchDevice(batch_id, device_id, DeviceSlotStatus.RESERVED)
        )

    def _reevaluate_batch(self, batch_id: str) -> None:
        """失败率超门限则熔断暂停（明确标记为 CIRCUIT）；收尾由 close_batch 显式触发。"""
        batch = self._storage.load_batch(batch_id)
        if batch is None or batch.status != BatchStatus.RUNNING:
            return
        tasks = self._storage.load_tasks(batch_id)
        if not tasks:
            return
        failed = sum(
            1 for t in tasks if t.status in (UpgradeStatus.FAILED, UpgradeStatus.TIMEOUT)
        )
        if failed / len(tasks) > batch.max_failure_ratio:
            batch.status = BatchStatus.PAUSED
            batch.pause_reason = PauseReason.CIRCUIT
            self._storage.save_batch(batch)
