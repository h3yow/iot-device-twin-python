"""固件升级：分批下发、状态跟踪、失败熔断。"""
from __future__ import annotations

import threading
import time
import uuid
from collections.abc import Callable

from app.models import (
    BatchStatus,
    TERMINAL_UPGRADE_STATES,
    UpgradeBatch,
    UpgradeStatus,
    UpgradeTask,
)
from app.storage import Storage


class UpgradeError(Exception):
    """升级约束被违反（版本门限/并发任务/批次状态/非法状态迁移）。"""


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
    - 批次 PAUSED/COMPLETED 后禁止继续下发；
    - 失败(含超时)设备数 / 批次任务总数 > max_failure_ratio 时批次自动 PAUSED，
      停止继续推进；
    - 任务状态机迁移非法时抛 UpgradeError。
    """

    def __init__(self, storage: Storage, clock: Callable[[], float] = time.time) -> None:
        self._storage = storage
        self._clock = clock
        self._lock = threading.Lock()

    def create_batch(
        self, batch_id: str, to_version: str, min_from_version: str, max_failure_ratio: float
    ) -> UpgradeBatch:
        parse_version(to_version)
        parse_version(min_from_version)
        if not 0.0 <= max_failure_ratio <= 1.0:
            raise UpgradeError("max_failure_ratio must be in [0, 1]")
        with self._lock:
            if self._storage.load_batch(batch_id) is not None:
                raise UpgradeError(f"batch already exists: {batch_id}")
            batch = UpgradeBatch(
                batch_id=batch_id,
                to_version=to_version,
                min_from_version=min_from_version,
                max_failure_ratio=max_failure_ratio,
                status=BatchStatus.RUNNING,
            )
            self._storage.save_batch(batch)
            return batch

    def dispatch(self, batch_id: str, device_id: str, from_version: str) -> UpgradeTask:
        """下发升级；违反门限/并发约束时抛 UpgradeError。"""
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
            self._storage.save_batch(batch)
            return batch

    # ---- 内部 ----
    def _find_task(self, task_id: str) -> UpgradeTask:
        for t in self._storage.load_tasks():
            if t.task_id == task_id:
                return t
        raise UpgradeError(f"unknown task: {task_id}")

    def _reevaluate_batch(self, batch_id: str) -> None:
        """失败率超门限则熔断暂停；批次收尾由 close_batch 显式触发。"""
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
            self._storage.save_batch(batch)
