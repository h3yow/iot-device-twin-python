"""设备孪生管理。

并发规则：
- 每设备一把互斥锁，同一设备的遥测应用与孪生更新串行化；
- update_reported 支持 expected_version 乐观校验，陈旧写入抛 ConflictError。

收敛规则：
- 遥测按 (ts, seq) 判定新旧；新事件覆盖 reported 并推进水位；
- 重复键 (ts, seq) 幂等忽略；乱序/迟到事件持久化但不覆盖最新状态；
- 重启后重放 telemetry.jsonl（文件顺序即接收顺序），最终状态与运行期一致。

在线判定：last_seen 距当前时间不超过 ONLINE_TTL_MS 即为在线。
"""
from __future__ import annotations

import threading
from contextlib import contextmanager
from typing import Any, Iterator

from .models import TelemetryEvent, TelemetryOutcome, TwinState
from .storage import Storage

ONLINE_TTL_MS = 60_000  # 超过该时长未上报视为离线
_SEEN_KEY_LIMIT = 4096  # 单设备去重键集合上限，超出后淘汰最旧键


class ConflictError(Exception):
    """版本冲突：存在并发写入，调用方需携带新版本号重试。"""


class TwinManager:
    """孪生状态的并发安全读写与恢复。"""

    def __init__(self, storage: Storage) -> None:
        self.storage = storage
        self._twins: dict[str, TwinState] = {}
        self._seen: dict[str, set[tuple[int, int]]] = {}
        self._locks: dict[str, threading.Lock] = {}
        self._locks_guard = threading.Lock()

    def lock_for(self, device_id: str) -> threading.Lock:
        with self._locks_guard:
            lock = self._locks.get(device_id)
            if lock is None:
                lock = threading.Lock()
                self._locks[device_id] = lock
            return lock

    @contextmanager
    def device_lock(self, device_id: str) -> Iterator[None]:
        with self.lock_for(device_id):
            yield

    # ---- 重启恢复 ----
    def recover(self, now_ms: int) -> None:
        """从持久化数据重建孪生：快照打底 + 全量遥测重放。"""
        self._twins = self.storage.load_twins()
        self._seen = {}
        # 重放期间使用接收顺序；迟到/重复判定规则与运行期完全一致
        for event in self.storage.load_telemetry():
            self._admit(event, persist=False)
        for twin in self._twins.values():
            twin.online = self._compute_online(twin, now_ms)
        if self._twins:
            self.storage.save_twins(self._twins)

    @staticmethod
    def _compute_online(twin: TwinState, now_ms: int) -> bool:
        return twin.last_seen > 0 and now_ms - twin.last_seen <= ONLINE_TTL_MS

    def _twin_of(self, device_id: str) -> TwinState:
        twin = self._twins.get(device_id)
        if twin is None:
            twin = TwinState(device_id=device_id)
            self._twins[device_id] = twin
        return twin

    # ---- 遥测应用（调用方须持有该设备锁）----
    def admit_and_persist(self, event: TelemetryEvent) -> TelemetryOutcome:
        """串行化地去重、判定并持久化遥测事件。"""
        return self._admit(event, persist=True)

    def _admit(self, event: TelemetryEvent, *, persist: bool) -> TelemetryOutcome:
        key = (event.ts, event.seq)
        seen = self._seen.setdefault(event.device_id, set())
        twin = self._twin_of(event.device_id)

        if key in seen:
            return TelemetryOutcome.DUPLICATE

        is_newer = (
            event.ts > twin.last_ts
            or (event.ts == twin.last_ts and event.seq > twin.last_seq)
        )
        # 先落日志：任何被接受的遥测（含迟到）都可用于审计与重放
        if persist:
            self.storage.append_telemetry(event)
        seen.add(key)
        if len(seen) > _SEEN_KEY_LIMIT:
            # 淘汰明显陈旧的键，保证内存有界
            kept = {k for k in seen if k >= (twin.last_ts, twin.last_seq)}
            self._seen[event.device_id] = kept

        twin.last_seen = max(twin.last_seen, event.received_at)
        if is_newer:
            twin.last_ts = event.ts
            twin.last_seq = event.seq
            # 仅用最新负载中的字段覆盖同名键，保留其他孪生字段，
            # 使并发 PATCH 与遥测收敛到同一份单调合并状态
            twin.reported.update(event.payload)
            twin.version += 1
            twin.online = True
            if persist:
                self.storage.save_twin(twin)
            return TelemetryOutcome.APPLIED

        if persist:
            self.storage.save_twin(twin)
        return TelemetryOutcome.LATE_OUT_OF_ORDER

    # ---- 并发更新 ----
    def update_reported(self, device_id: str, patch: dict[str, Any],
                        expected_version: int | None = None) -> TwinState:
        """合并 reported 并推进版本；版本不匹配抛 ConflictError。"""
        with self.lock_for(device_id):
            twin = self._twin_of(device_id)
            if expected_version is not None and twin.version != expected_version:
                raise ConflictError(
                    f"version conflict: expected {expected_version}, "
                    f"current {twin.version}"
                )
            merged = dict(twin.reported)
            merged.update(patch)
            twin.reported = merged
            twin.version += 1
            self.storage.save_twin(twin)
            return self._snapshot(twin)

    def get(self, device_id: str, now_ms: int | None = None) -> TwinState | None:
        with self.lock_for(device_id):
            twin = self._twins.get(device_id)
            if twin is None:
                return None
            if now_ms is not None:
                twin.online = self._compute_online(twin, now_ms)
            return self._snapshot(twin)

    def refresh_online(self, now_ms: int) -> None:
        for device_id in list(self._twins):
            with self.lock_for(device_id):
                twin = self._twins[device_id]
                twin.online = self._compute_online(twin, now_ms)

    @staticmethod
    def _snapshot(twin: TwinState) -> TwinState:
        return TwinState(
            device_id=twin.device_id,
            reported=dict(twin.reported),
            version=twin.version,
            last_ts=twin.last_ts,
            last_seq=twin.last_seq,
            last_seen=twin.last_seen,
            online=twin.online,
        )
