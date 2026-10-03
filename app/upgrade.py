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
    - 设备名额：RUNNING 批次内（设备名下有非 CANCELLED 任务，含其他终态）
      的设备视为被该批次占用；暂停/熔断/收尾后不再占用，释放给其他批次。
      其他 RUNNING 批次想下发被占用的设备时，只有当自己优先级更高
      （priority 大；相同则创建更早）才能抢到名额；低优先级批次既不能
      抢走设备，也不会留下任何中间状态；
    - 抢占：严格更高优先级的批次成为属主时，设备上低优先级批次的
      进行中任务会被立即置为 CANCELLED（终态，原批次内永不重发），
      保证同一设备同一时刻只有属主批次的一个进行中任务；已暂停/熔断/
      收尾批次遗留的在途任务同样被抢占（名额已释放，不把设备挂住）；
      同优先级 RUNNING 批次的在途任务不可抢，先占先得；
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
        """下发升级；违反门限/并发/批次状态/名额争抢约束时抛 UpgradeError。

        全部判定与落盘在同一把锁内完成：被挡下时不会创建任何任务，
        不留下半推半就的中间状态。
        """
        with self._lock:
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
            # 同批次内设备唯一：已下发过的设备（含失败/超时等终态）不再下发
            existing = self._storage.task_for_batch_device(batch_id, device_id)
            if existing is not None:
                raise UpgradeError(
                    f"device {device_id} already dispatched in batch {batch_id} "
                    f"as task {existing.task_id} ({existing.status.value})"
                )
            # 名额争抢：设备在任一时刻只有一个属主批次——所有 RUNNING 且
            # 名下已有该设备任务的批次，加上当前请求批次，一起按
            # 优先级/创建时间排名；排名最高者才是属主。当前批次只有本身
            # 就是属主时才能下发，即高优先级可以抢占、低优先级不能从属主
            # 手里抢设备；整个判定在同一把锁内完成，被挡下时不创建任务，
            # 不留下半推半就的中间状态。
            holder = self._holder_of(device_id, contender=batch)
            if holder.batch_id != batch_id:
                raise UpgradeError(
                    f"device {device_id} held by batch {holder.batch_id} "
                    f"(priority {holder.priority}); batch {batch_id} "
                    f"priority {batch.priority} cannot preempt"
                )
            # 抢占：本批次是属主，但设备上可能还挂着其他批次的进行中任务
            # （低优先级批次先下发的，或已暂停/熔断批次遗留的在途任务）。
            # 严格更高优先级可以抢占在途任务——被取消的任务置为 CANCELLED
            # （终态，原批次内永不重发，迟到回报也会被状态机拒绝），保证
            # 同一设备同一时刻只有属主批次的一个进行中任务；对方已暂停/
            # 熔断/收尾（名额已释放）的在途任务同样取消，不把设备挂住。
            # 同优先级或更高优先级的 RUNNING 批次在途任务不可抢，直接挡下。
            for active in self._storage.active_tasks_for_device(device_id):
                if active.batch_id == batch_id:
                    continue  # 理论不可达（同批次唯一性已校验），防御性跳过
                owner = self._storage.load_batch(active.batch_id)
                if (
                    owner is not None
                    and owner.status == BatchStatus.RUNNING
                    and owner.priority >= batch.priority
                ):
                    raise UpgradeError(
                        f"device {device_id} already has active task "
                        f"{active.task_id} owned by batch {owner.batch_id} "
                        f"(priority {owner.priority}); batch {batch_id} "
                        f"priority {batch.priority} cannot preempt inflight"
                    )
                active.status = UpgradeStatus.CANCELLED
                active.updated_at = self._clock()
                self._storage.save_task(active)
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

    def _holder_of(
        self, device_id: str, contender: UpgradeBatch | None = None
    ) -> UpgradeBatch | None:
        """设备当前的属主批次：RUNNING 且名下有该设备占用任务的批次中排名最高者。

        占用任务 = 该批次名下该设备的非 CANCELLED 任务（任意其他状态，含
        终态）；被抢占/取消（CANCELLED）的任务只是终态历史记录，不再占用
        名额。排名即 load_batches 的返回顺序（priority 降序、created_at
        升序、batch_id 升序）；暂停/熔断/收尾的批次不参与，故暂停后名额
        自然顺延。contender 作为尚未落任务行的虚拟候选（下发请求方）一起
        参与排名。
        """
        best: UpgradeBatch | None = None
        for candidate in self._storage.load_batches():
            if candidate.status != BatchStatus.RUNNING:
                continue
            if candidate.batch_id == (contender.batch_id if contender is not None else None):
                c = contender  # 用内存中的请求批次（含最新 priority/created_at）
            elif self._storage.occupying_task_for_batch_device(
                candidate.batch_id, device_id
            ) is None:
                continue
            else:
                c = candidate
            if best is None:
                best = c
        return best if best is not None else contender

    def get_batch(self, batch_id: str) -> UpgradeBatch:
        """查询批次当前状态与暂停原因（manual/circuit_breaker/None）。"""
        with self._lock:
            batch = self._storage.load_batch(batch_id)
            if batch is None:
                raise UpgradeError(f"unknown batch: {batch_id}")
            return batch

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
