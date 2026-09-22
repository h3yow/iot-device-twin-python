"""遥测上行接入。

接入语义（判定依据随结果返回）：
- 非法 JSON -> UNPARSEABLE_PAYLOAD（400）
- 缺少 ts / seq / data 等必填字段 -> MISSING_FIELD（422）
- 字段类型不兼容（如 ts 非整数、data 含嵌套对象/数组）
  -> INCOMPATIBLE_FIELD_TYPE（422）
- 设备未注册 -> UNKNOWN_DEVICE（404）
- 完全相同的 (ts, seq) 重复上报 -> DUPLICATE（200，幂等忽略）
- 落后于已应用水位的乱序/迟到上报 -> LATE_OUT_OF_ORDER（200，落日志但不覆盖）
- 其余 -> APPLIED（200，更新最新孪生状态）
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .models import TelemetryEvent, TelemetryOutcome, TelemetryRejectReason
from .util import now_ms
from .twin import TwinManager
from .storage import Storage

_SCALAR_TYPES = (int, float, str, bool, type(None))


@dataclass
class IngestResult:
    ok: bool
    outcome: TelemetryOutcome | None = None
    reject_reason: TelemetryRejectReason | None = None
    detail: str = ""
    event: TelemetryEvent | None = None


class TelemetryIngestor:
    """解析、校验、去重并按序应用遥测。"""

    def __init__(self, storage: Storage, twins: TwinManager) -> None:
        self.storage = storage
        self.twins = twins

    def ingest(self, device_id: str, raw: bytes | str | dict[str, Any]) -> IngestResult:
        # 1) 解析
        if isinstance(raw, dict):
            body: Any = raw
        else:
            try:
                import json
                body = json.loads(raw)
            except (ValueError, TypeError) as exc:
                return IngestResult(False, reject_reason=TelemetryRejectReason.UNPARSEABLE_PAYLOAD,
                                    detail=f"invalid json: {exc}")
        if not isinstance(body, dict):
            return IngestResult(False, reject_reason=TelemetryRejectReason.INCOMPATIBLE_FIELD_TYPE,
                                detail="top-level payload must be a JSON object")

        # 2) 必填字段
        for field_name in ("ts", "seq", "data"):
            if field_name not in body:
                return IngestResult(False, reject_reason=TelemetryRejectReason.MISSING_FIELD,
                                    detail=f"missing required field: {field_name}")

        ts, seq, data = body["ts"], body["seq"], body["data"]
        # bool 是 int 的子类，显式排除
        if not isinstance(ts, int) or isinstance(ts, bool):
            return IngestResult(False, reject_reason=TelemetryRejectReason.INCOMPATIBLE_FIELD_TYPE,
                                detail="ts must be an integer (epoch milliseconds)")
        if not isinstance(seq, int) or isinstance(seq, bool) or seq < 0:
            return IngestResult(False, reject_reason=TelemetryRejectReason.INCOMPATIBLE_FIELD_TYPE,
                                detail="seq must be a non-negative integer")
        if not isinstance(data, dict):
            return IngestResult(False, reject_reason=TelemetryRejectReason.INCOMPATIBLE_FIELD_TYPE,
                                detail="data must be a JSON object of scalar fields")
        for key, value in data.items():
            if not isinstance(key, str) or not isinstance(value, _SCALAR_TYPES):
                return IngestResult(
                    False, reject_reason=TelemetryRejectReason.INCOMPATIBLE_FIELD_TYPE,
                    detail=f"data.{key} must be a scalar value")

        # 3) 设备注册校验
        if device_id not in self.storage.load_devices():
            return IngestResult(False, reject_reason=TelemetryRejectReason.UNKNOWN_DEVICE,
                                detail=f"device '{device_id}' is not registered")

        # 4) 串行化受理（去重 / 乱序判定在每设备锁内完成）
        event = TelemetryEvent(
            device_id=device_id, ts=ts, seq=seq, payload=data,
            received_at=now_ms(),
        )
        with self.twins.device_lock(device_id):
            outcome = self.twins.admit_and_persist(event)
        return IngestResult(True, outcome=outcome, event=event,
                            detail=f"accepted as {outcome.value}")
