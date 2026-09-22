"""共享类型定义：遥测、孪生、升级领域模型与枚举。"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any


class TelemetryRejectReason(enum.Enum):
    """遥测被拒绝的可区分原因。"""

    INVALID_JSON = "invalid_json"
    MISSING_FIELD = "missing_field"
    INVALID_TYPE = "invalid_type"
    INVALID_VERSION = "invalid_version"
    UNKNOWN_DEVICE = "unknown_device"


class IngestResult(enum.Enum):
    """遥测接入结果。"""

    APPLIED = "applied"            # 新数据，已应用
    DUPLICATE = "duplicate"        # 重复上报，幂等忽略
    STALE = "stale"                # 乱序/迟到的旧数据，忽略
    REJECTED = "rejected"          # 负载非法，拒绝


class UpgradeStatus(enum.Enum):
    """单设备升级任务状态机。"""

    PENDING = "pending"
    DOWNLOADING = "downloading"
    FLASHING = "flashing"
    SUCCESS = "success"
    FAILED = "failed"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"


TERMINAL_UPGRADE_STATES = frozenset(
    {UpgradeStatus.SUCCESS, UpgradeStatus.FAILED, UpgradeStatus.TIMEOUT, UpgradeStatus.CANCELLED}
)


class BatchStatus(enum.Enum):
    """升级批次状态。"""

    RUNNING = "running"
    PAUSED = "paused"      # 失败率超门限，停止推进
    COMPLETED = "completed"


@dataclass(frozen=True)
class TelemetryRecord:
    device_id: str
    seq: int                 # 设备侧单调递增序号
    schema_version: int      # 负载 schema 版本
    payload: dict[str, Any]
    received_at: float       # 服务端接收时间戳


@dataclass
class TwinState:
    device_id: str
    reported: dict[str, Any] = field(default_factory=dict)
    desired: dict[str, Any] = field(default_factory=dict)
    reported_version: int = 0   # 已应用的最大遥测 seq
    desired_version: int = 0    # desired 单调递增版本
    last_seen: float = 0.0      # 最近一次遥测接收时间
    online: bool = False


@dataclass
class UpgradeTask:
    task_id: str
    device_id: str
    batch_id: str
    from_version: str
    to_version: str
    status: UpgradeStatus
    updated_at: float


@dataclass
class UpgradeBatch:
    batch_id: str
    to_version: str
    min_from_version: str      # 版本门限：低于该版本的设备不允许进入本批次
    max_failure_ratio: float   # 失败率门限，超过则暂停推进
    status: BatchStatus
