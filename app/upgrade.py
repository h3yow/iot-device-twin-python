"""固件升级：分批下发、状态跟踪、失败熔断。"""
from __future__ import annotations

import threading
import time
import uuid
from collections.abc import Callable

from app.models import (
    BatchStatus,
    PauseReason,
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
    - 同一批次内同一台设备至多被下发一次：已完成/失败/超时/取消的设备，
      在该批次内永远不会再次收到任务；
    - 设备当前版本必须 >= 批次 min_from_version（版本门限）；
    - 批次仅 RUNNING 时可下发；PAUSED（手动暂停或熔断）/COMPLETED 一律停止；
    - 暂停（pause_batch）只阻止新下发，进行中的任务允许自然结束；
      恢复（resume_batch）后按原进度继续推进；
    - 失败(含超时)设备数 / 批次任务总数 > max_failure_ratio 时批次自动熔断，
      状态 PAUSED 且 pause_reason=circuit_breaker，停止继续推进；
    - 设备名额（硬占用）：RUNNING 且名下已有该设备任务的批次参与属主排名
      （priority 大优先；相同则 created_at 早、batch_id 小者优先）。同一台
      设备在任一时刻只有一个属主批次；当前批次只有排名为属主时才能下发，
      低优批次不能从属主手里抢设备，被挡下时不创建任何任务，不留中间状态；
    - 抢占：高优（或同优更早创建）批次要占用已被在途任务占住的设备时，
      原在途任务被干净地置为 cancelled（不计失败、不参与熔断），设备立即
      易主，不必等原任务自然结束或超时；被抢占批次名下留有该 cancelled
      终态记录，因此它之后（含暂停恢复后）再也不能向该设备下发；
    - 释放：批次暂停（手动/熔断）/收尾后立即退出属主排名，它原先占用的
      设备马上可以被其他 RUNNING 批次接管；暂停操作本身不主动取消在途
      任务（无人争抢时它们仍可自然结束），一旦其他 RUNNING 批次来抢，
      在途任务立即被抢占取消；
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
                created_at=self._clock(),
            )
            self._storage.save_batch(batch)
            return batch

    def dispatch(self, batch_id: str, device_id: str, from_version: str) -> UpgradeTask:
        """下发升级；返回新任务。详见 dispatch_with_preemption。"""
        task, _ = self.dispatch_with_preemption(batch_id, device_id, from_version)
        return task

    def dispatch_with_preemption(
        self, batch_id: str, device_id: str, from_version: str
    ) -> tuple[UpgradeTask, UpgradeTask | None]:
        """下发升级，返回 (新任务, 被抢占取消的旧任务|None)。

        违反门限/并发/批次状态/名额争抢约束时抛 UpgradeError。全部判定与
        落盘在同一把锁内完成：被挡下时不会创建任何任务，不留下半推半就的
        中间状态。当前批次赢得属主排名但设备上还有其他批次在途任务时
        （高优抢占，或原属主已暂停/收尾），原任务被干净地置为 cancelled
        后再创建新任务。
        """
        with self._lock:
            return self._dispatch_locked(batch_id, device_id, from_version)

    def _dispatch_locked(
        self, batch_id: str, device_id: str, from_version: str
    ) -> tuple[UpgradeTask, UpgradeTask | None]:
        batch = self._storage.load_batch(batch_id)
        if batch is None:
            raise UpgradeError(f"unknown batch: {batch_id}")
        if batch.status != BatchStatus.RUNNING:
            raise UpgradeError(
                f"batch {batch_id} is {batch.status.value}"
                + (
                    f" ({batch.pause_reason.value})"
                    if batch.pause_reason is not None
                    else ""
                )
                + ", dispatch stopped"
            )
        if parse_version(from_version) < parse_version(batch.min_from_version):
            raise UpgradeError(
                f"device {device_id} version {from_version} below gate "
                f"{batch.min_from_version}"
            )
        # 同批次内设备唯一：已下发过的设备（含失败/超时/被抢占取消等终态）
        # 不再下发
        existing = self._storage.task_for_batch_device(batch_id, device_id)
        if existing is not None:
            raise UpgradeError(
                f"device {device_id} already dispatched in batch {batch_id} "
                f"as task {existing.task_id} ({existing.status.value})"
            )
        # 名额争抢：先排名再处理在途任务——所有 RUNNING 且名下已有该设备
        # 任务的批次，加上当前请求批次，按优先级/创建时间排名；排名最高者
        # 才是属主。当前批次不是属主则直接拒绝，不创建任务、不留中间状态。
        # 注意必须在"全局进行中任务互斥"检查之前排名，否则低优先下发的
        # 在途任务会把后到的高优请求死挡回去。
        holder = self._holder_of(device_id, contender=batch)
        if holder is None or holder.batch_id != batch_id:
            raise UpgradeError(
                f"device {device_id} held by batch {holder.batch_id if holder else None} "
                f"(priority {holder.priority if holder else None}); batch {batch_id} "
                f"priority {batch.priority} cannot preempt"
            )
        # 当前批次是属主：设备上若还有其他批次的在途任务（高优抢占低优，
        # 或原属主批次已暂停/熔断/收尾后名额顺延），干净地取消它再接管。
        # 抢占取消不走状态机迁移表（cancelled 可从任意非终态直接到达），
        # 且不计入失败率、不触发熔断。
        active = self._storage.active_task_for_device(device_id)
        preempted: UpgradeTask | None = None
        if active is not None and active.batch_id != batch_id:
            active.status = UpgradeStatus.CANCELLED
            active.updated_at = self._clock()
            self._storage.save_task(active)
            preempted = active
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
        return task, preempted

    def get_batch(self, batch_id: str) -> UpgradeBatch:
        """查询批次当前状态（running/paused/completed）及暂停原因。

        未知批次抛 UpgradeError，供调用方区分手动暂停、熔断暂停与正常完成，
        而不必通过下发报错反推。
        """
        with self._lock:
            batch = self._storage.load_batch(batch_id)
            if batch is None:
                raise UpgradeError(f"unknown batch: {batch_id}")
            return batch

    def list_batches(self) -> list[UpgradeBatch]:
        """列出全部批次，顺序即名额争抢排名（priority 降序、创建时间升序）。"""
        with self._lock:
            return self._storage.load_batches()

    def _holder_of(
        self, device_id: str, contender: UpgradeBatch | None = None
    ) -> UpgradeBatch | None:
        """设备当前的属主批次：RUNNING 且名下有该设备任务的批次中排名最高者。

        排名即 load_batches 的返回顺序（priority 降序、created_at 升序、
        batch_id 升序）；暂停/熔断/收尾的批次不参与，故暂停后名额自然顺延。
        contender 作为尚未落任务行的虚拟候选（下发请求方）一起参与排名。
        """
        best: UpgradeBatch | None = None
        for candidate in self._storage.load_batches():
            if candidate.status != BatchStatus.RUNNING:
                continue
            if candidate.batch_id == (contender.batch_id if contender is not None else None):
                c = contender  # 用内存中的请求批次（含最新 priority/created_at）
            elif self._storage.task_for_batch_device(candidate.batch_id, device_id) is None:
                continue
            else:
                c = candidate
            if best is None:
                best = c
        return best if best is not None else contender

    def pause_batch(self, batch_id: str) -> UpgradeBatch:
        """手动暂停批次：停止新下发，进行中任务允许自然结束。

        已经因熔断暂停的批次保持熔断原因不变（手动暂停不能覆盖熔断事实）。
        """
        with self._lock:
            batch = self._storage.load_batch(batch_id)
            if batch is None:
                raise UpgradeError(f"unknown batch: {batch_id}")
            if batch.status == BatchStatus.COMPLETED:
                raise UpgradeError(f"batch {batch_id} already completed")
            if batch.status == BatchStatus.RUNNING:
                batch.status = BatchStatus.PAUSED
                batch.pause_reason = PauseReason.MANUAL
                self._storage.save_batch(batch)
            return batch

    def resume_batch(self, batch_id: str) -> UpgradeBatch:
        """恢复暂停的批次：接着原进度推进，已终态设备不会被重新下发。"""
        with self._lock:
            batch = self._storage.load_batch(batch_id)
            if batch is None:
                raise UpgradeError(f"unknown batch: {batch_id}")
            if batch.status != BatchStatus.PAUSED:
                raise UpgradeError(
                    f"batch {batch_id} is {batch.status.value}, only paused can resume"
                )
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
            batch.pause_reason = PauseReason.CIRCUIT_BREAKER
            self._storage.save_batch(batch)
