"""设备孪生：并发安全更新与重启恢复。"""
from __future__ import annotations

import threading
import time
from collections.abc import Callable

from app.models import TwinState
from app.storage import Storage

ONLINE_TTL_SECONDS = 60.0


class ConflictError(Exception):
    """孪生版本冲突，拒绝陈旧覆盖。"""


class TwinService:
    """孪生收敛规则：

    - reported 只接受 seq 严格大于 reported_version 的遥测（单调推进，乱序不覆盖）；
    - desired 采用乐观并发控制：expected_version 不匹配即抛 ConflictError；
    - 每次变更立即落盘，重启后 recover() 从 SQLite 重建；
    - online 由 last_seen 与当前时间的差值动态判定，不持久化，
      重启后按持久化的 last_seen 一致地重建。
    """

    def __init__(
        self,
        storage: Storage,
        clock: Callable[[], float] = time.time,
        online_ttl: float = ONLINE_TTL_SECONDS,
    ) -> None:
        self._storage = storage
        self._clock = clock
        self._online_ttl = online_ttl
        self._twins: dict[str, TwinState] = {}
        self._locks: dict[str, threading.Lock] = {}
        self._guard = threading.Lock()  # 保护 _twins / _locks 结构本身

    def recover(self) -> None:
        """从持久化数据重建内存孪生状态。"""
        with self._guard:
            self._twins = self._storage.load_twins()
            self._locks = {d: threading.Lock() for d in self._twins}

    def _lock_for(self, device_id: str) -> threading.Lock:
        with self._guard:
            return self._locks.setdefault(device_id, threading.Lock())

    def apply_reported(self, device_id: str, seq: int, payload: dict, received_at: float) -> None:
        """应用一条遥测到 reported；陈旧 seq 直接丢弃，绝不回退。"""
        with self._lock_for(device_id):
            twin = self._twins.get(device_id)
            if twin is None:
                twin = TwinState(device_id=device_id)
                self._twins[device_id] = twin
            if seq <= twin.reported_version:
                return  # 陈旧覆盖防护
            twin.reported.update(payload)
            twin.reported_version = seq
            twin.last_seen = max(twin.last_seen, received_at)
            self._storage.save_twin(twin)

    def update_desired(
        self, device_id: str, patch: dict, expected_version: int | None = None
    ) -> TwinState:
        """乐观并发控制：expected_version 不匹配时抛 ConflictError。"""
        with self._lock_for(device_id):
            twin = self._twins.get(device_id)
            if twin is None:
                twin = TwinState(device_id=device_id)
                self._twins[device_id] = twin
            if expected_version is not None and expected_version != twin.desired_version:
                raise ConflictError(
                    f"desired version conflict for {device_id}: "
                    f"expected={expected_version} actual={twin.desired_version}"
                )
            twin.desired.update(patch)
            twin.desired_version += 1
            self._storage.save_twin(twin)
            return twin

    def get(self, device_id: str) -> TwinState | None:
        with self._guard:
            twin = self._twins.get(device_id)
        if twin is None:
            return None
        twin.online = (self._clock() - twin.last_seen) < self._online_ttl
        return twin
