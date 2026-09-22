"""领域模型与枚举定义（骨架：仅类型，无实现）。"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any


class TelemetryRejectReason(enum.Enum):
    """遥测被拒绝的可区分原因。"""

    UNPARSEABLE_PAYLOAD = "unparseable_payload"
    MISSING_FIELD = "missing_field"
    INCOMPATIBLE_FIELD_TYPE = "incompatible_field_type"
    UNKNOWN_DEVICE = "unknown_device"


class TelemetryOutcome(enum.Enum):
    """遥测受理结果（区别于拒绝）。"""

    APPLIED = "applied"                # 已应用为最新状态
    DUPLICATE = "duplicate"            # 重复上报，已去重忽略
    LATE_OUT_OF_ORDER = "late"         # 乱序/迟到，已持久化但不覆盖最新状态


class OtaStatus(enum.Enum):
    """升级任务状态机。"""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    SUCCESS = "success"
    FAILED = "failed"
    TIMED_OUT = "timed_out"
    ABORTED = "aborted"


# 允许的状态迁移表（ota.py 中实现校验）
OTA_TRANSITIONS: dict[OtaStatus, set[OtaStatus]] = {
    OtaStatus.PENDING: {OtaStatus.IN_PROGRESS, OtaStatus.ABORTED},
    OtaStatus.IN_PROGRESS: {OtaStatus.SUCCESS, OtaStatus.FAILED, OtaStatus.TIMED_OUT, OtaStatus.ABORTED},
    OtaStatus.SUCCESS: set(),
    OtaStatus.FAILED: set(),
    OtaStatus.TIMED_OUT: set(),
    OtaStatus.ABORTED: set(),
}

OTA_TERMINAL: set[OtaStatus] = {
    OtaStatus.SUCCESS,
    OtaStatus.FAILED,
    OtaStatus.TIMED_OUT,
    OtaStatus.ABORTED,
}


@dataclass
class TelemetryEvent:
    """一条已持久化的遥测事件。"""

    device_id: str
    ts: int                 # 设备侧时间戳（毫秒）
    seq: int                # 设备侧单调序号
    payload: dict[str, Any]
    received_at: int = 0    # 服务端接收时间（毫秒）


@dataclass
class TwinState:
    """设备孪生状态。"""

    device_id: str
    reported: dict[str, Any] = field(default_factory=dict)
    version: int = 0            # 乐观并发控制版本号
    last_ts: int = -1           # 已应用的最大设备时间戳
    last_seq: int = -1          # 已应用的最大序号
    last_seen: int = 0          # 最近一次收到遥测的服务端时间
    online: bool = False


@dataclass
class UpgradeTask:
    """单设备升级任务。"""

    task_id: str
    device_id: str
    batch_id: str
    from_version: str
    to_version: str
    status: OtaStatus = OtaStatus.PENDING
    attempts: int = 0
    created_at: int = 0
    updated_at: int = 0


@dataclass
class Batch:
    """升级批次。"""

    batch_id: str
    to_version: str
    min_current_version: str   # 版本门限：低于该版本的设备不得进入批次
    max_concurrent: int        # 批次内同时进行中的升级上限
    device_ids: list[str] = field(default_factory=list)
