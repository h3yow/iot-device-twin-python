"""遥测上行接入：校验、去重、乱序收敛。"""
from __future__ import annotations

import json
import time
from collections.abc import Callable
from typing import Any

from app.models import IngestResult, TelemetryRecord, TelemetryRejectReason
from app.storage import Storage

SUPPORTED_SCHEMA_VERSION = 1

_REQUIRED_FIELDS = ("device_id", "seq", "schema_version", "payload")


class TelemetryService:
    """遥测接入语义：

    - 负载必须是 JSON 对象，含 device_id/seq/schema_version/payload 且类型正确；
    - schema_version 必须等于当前支持版本，否则拒绝（INVALID_VERSION）；
    - (device_id, seq) 唯一：重复上报幂等忽略（DUPLICATE）；
    - seq 小于该设备已见最大序号：乱序/迟到，记录但不更新状态（STALE）；
    - 仅当 seq 严格大于已应用序号时才推进孪生（APPLIED）。
    """

    def __init__(
        self,
        storage: Storage,
        on_applied: Callable[[str, int, dict, float], None] | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._storage = storage
        self._on_applied = on_applied
        self._clock = clock

    def ingest(self, raw_body: bytes) -> tuple[IngestResult, TelemetryRejectReason | None]:
        """接入一条遥测。返回 (结果, 拒绝原因)；拒绝原因仅当 REJECTED 时非空。"""
        try:
            data = json.loads(raw_body)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return IngestResult.REJECTED, TelemetryRejectReason.INVALID_JSON
        if not isinstance(data, dict):
            return IngestResult.REJECTED, TelemetryRejectReason.INVALID_JSON

        for f in _REQUIRED_FIELDS:
            if f not in data:
                return IngestResult.REJECTED, TelemetryRejectReason.MISSING_FIELD

        device_id, seq, ver, payload = (
            data["device_id"], data["seq"], data["schema_version"], data["payload"],
        )
        if (
            not isinstance(device_id, str)
            or not device_id
            or not isinstance(seq, int)
            or isinstance(seq, bool)
            or seq < 0
            or not isinstance(ver, int)
            or isinstance(ver, bool)
            or not isinstance(payload, dict)
        ):
            return IngestResult.REJECTED, TelemetryRejectReason.INVALID_TYPE

        if ver != SUPPORTED_SCHEMA_VERSION:
            return IngestResult.REJECTED, TelemetryRejectReason.INVALID_VERSION

        received_at = self._clock()
        record = TelemetryRecord(device_id, seq, ver, payload, received_at)
        inserted = self._storage.insert_telemetry(record)
        if not inserted:
            return IngestResult.DUPLICATE, None

        if seq < self._storage.max_seq(device_id):
            return IngestResult.STALE, None

        if self._on_applied is not None:
            self._on_applied(device_id, seq, payload, received_at)
        return IngestResult.APPLIED, None
