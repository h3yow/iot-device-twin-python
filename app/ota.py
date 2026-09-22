"""固件升级分批下发与状态跟踪。

状态机（见 models.OTA_TRANSITIONS）：
  PENDING -> IN_PROGRESS -> SUCCESS / FAILED / TIMED_OUT
  PENDING / IN_PROGRESS -> ABORTED
  终态之间不得再迁移；重复上报同一终态返回 already_terminal。

推进规则（dispatch）：
- 同一设备任意时刻至多一个非终态任务（跨批次同样生效），否则 device_busy；
- 设备在本批次已有任务（成功/失败/超时/中止）不再重复推进；
- 批次内 IN_PROGRESS 数量达到 max_concurrent 时停止推进；
- 版本门限：设备当前版本（孪生 reported.fw_version）必须
  >= batch.min_current_version，否则 below_version_gate，不得下发。
失败/超时设备 sweep/report 后可被识别，dispatch 永不再次推进。
"""
from __future__ import annotations

import threading
from dataclasses import dataclass, field

from .models import (
    Batch,
    OTA_TERMINAL,
    OTA_TRANSITIONS,
    OtaStatus,
    UpgradeTask,
)
from .storage import Storage
from .twin import TwinManager
from .util import parse_version


class OtaError(Exception):
    """升级规则被违反时抛出，携带可区分的错误码。"""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass
class DispatchResult:
    started: list[UpgradeTask] = field(default_factory=list)
    skipped: list[tuple[str, str]] = field(default_factory=list)  # (device_id, reason)

    def skipped_reason(self, device_id: str) -> str | None:
        for did, reason in self.skipped:
            if did == device_id:
                return reason
        return None


class OtaManager:
    """批次与任务的创建、推进、状态迁移。"""

    def __init__(self, storage: Storage, twins: TwinManager) -> None:
        self.storage = storage
        self.twins = twins
        self.batches: dict[str, Batch] = {}
        self.tasks: dict[str, UpgradeTask] = {}
        self._lock = threading.Lock()

    # ---- 持久化与恢复 ----
    def _persist(self) -> None:
        self.storage.save_ota(self.batches, self.tasks)

    def recover(self) -> None:
        self.batches, self.tasks = self.storage.load_ota()

    # ---- 批次 ----
    def create_batch(self, batch_id: str, to_version: str,
                     min_current_version: str, max_concurrent: int,
                     device_ids: list[str]) -> Batch:
        with self._lock:
            if batch_id in self.batches:
                raise OtaError("batch_exists", f"batch '{batch_id}' already exists")
            if max_concurrent <= 0:
                raise OtaError("invalid_concurrency", "max_concurrent must be positive")
            try:
                parse_version(to_version)
                parse_version(min_current_version)
            except ValueError as exc:
                raise OtaError("invalid_version", str(exc)) from exc
            batch = Batch(
                batch_id=batch_id,
                to_version=to_version,
                min_current_version=min_current_version,
                max_concurrent=max_concurrent,
                device_ids=list(dict.fromkeys(device_ids)),
            )
            self.batches[batch_id] = batch
            self._persist()
            return batch

    # ---- 任务查询 ----
    def active_task_of(self, device_id: str) -> UpgradeTask | None:
        for task in self.tasks.values():
            if task.device_id == device_id and task.status not in OTA_TERMINAL:
                return task
        return None

    def _task_for(self, batch_id: str, device_id: str) -> UpgradeTask | None:
        return self.tasks.get(f"{batch_id}:{device_id}")

    # ---- 推进 ----
    def dispatch(self, batch_id: str, now_ms: int) -> DispatchResult:
        """在批次约束内尽量推进设备，返回已启动与被跳过（含原因）清单。"""
        with self._lock:
            batch = self.batches.get(batch_id)
            if batch is None:
                raise OtaError("batch_not_found", f"batch '{batch_id}' not found")

            in_progress = sum(
                1 for t in self.tasks.values()
                if t.batch_id == batch_id and t.status is OtaStatus.IN_PROGRESS
            )
            result = DispatchResult()

            for device_id in batch.device_ids:
                # 设备级约束先判定，确保批次门限等失败原因不被并发槽位掩盖
                existing = self._task_for(batch_id, device_id)
                if existing is not None:
                    if existing.status in OTA_TERMINAL:
                        # 成功/失败/超时/中止：不再推进，失败设备因此被识别并隔离
                        result.skipped.append(
                            (device_id, f"already_{existing.status.value}"))
                    else:
                        result.skipped.append((device_id, "already_dispatched"))
                    continue

                active = self.active_task_of(device_id)
                if active is not None:
                    result.skipped.append((device_id, "device_busy"))
                    continue

                gate_reason = self._check_version_gate(batch, device_id)
                if gate_reason is not None:
                    result.skipped.append((device_id, gate_reason))
                    continue

                # 设备可推进时才受批次并发上限约束
                if in_progress >= batch.max_concurrent:
                    result.skipped.append((device_id, "concurrency_limit"))
                    continue

                task = UpgradeTask(
                    task_id=f"{batch_id}:{device_id}",
                    device_id=device_id,
                    batch_id=batch_id,
                    from_version=self._fw_version(device_id) or "unknown",
                    to_version=batch.to_version,
                    status=OtaStatus.PENDING,
                    created_at=now_ms,
                    updated_at=now_ms,
                )
                self._transition(task, OtaStatus.IN_PROGRESS, now_ms)
                self.tasks[task.task_id] = task
                in_progress += 1
                result.started.append(task)

            self._persist()
            return result

    def _fw_version(self, device_id: str) -> str | None:
        twin = self.twins.get(device_id)
        if twin is None:
            return None
        value = twin.reported.get("fw_version")
        return value if isinstance(value, str) else None

    def _check_version_gate(self, batch: Batch, device_id: str) -> str | None:
        current = self._fw_version(device_id)
        if current is None:
            return "missing_fw_version"
        try:
            if parse_version(current) < parse_version(batch.min_current_version):
                return "below_version_gate"
        except ValueError:
            return "invalid_device_version"
        return None

    # ---- 状态迁移 ----
    def _transition(self, task: UpgradeTask, new_status: OtaStatus, now_ms: int) -> None:
        allowed = OTA_TRANSITIONS.get(task.status, set())
        if new_status not in allowed:
            if task.status in OTA_TERMINAL:
                # 终态收到任何上报（含相同状态的重复上报）都归为 already_terminal
                raise OtaError(
                    "already_terminal",
                    f"task {task.task_id} already in terminal state {task.status.value}, "
                    f"cannot report {new_status.value}")
            raise OtaError(
                "illegal_transition",
                f"cannot transition {task.status.value} -> {new_status.value} "
                f"for task {task.task_id}")
        task.status = new_status
        task.updated_at = now_ms

    def report_status(self, task_id: str, status: OtaStatus, now_ms: int) -> UpgradeTask:
        """设备上报升级结果；仅允许合法状态迁移。"""
        with self._lock:
            task = self.tasks.get(task_id)
            if task is None:
                raise OtaError("task_not_found", f"task '{task_id}' not found")
            self._transition(task, status, now_ms)
            if status in (OtaStatus.FAILED, OtaStatus.SUCCESS, OtaStatus.TIMED_OUT):
                task.attempts += 1
            self._persist()
            return task

    def sweep_timeouts(self, now_ms: int, timeout_ms: int) -> list[UpgradeTask]:
        """将超过 timeout_ms 仍处于 IN_PROGRESS 的任务标记为 TIMED_OUT。"""
        timed_out: list[UpgradeTask] = []
        with self._lock:
            for task in self.tasks.values():
                if (task.status is OtaStatus.IN_PROGRESS
                        and now_ms - task.updated_at > timeout_ms):
                    self._transition(task, OtaStatus.TIMED_OUT, now_ms)
                    timed_out.append(task)
            if timed_out:
                self._persist()
        return timed_out
