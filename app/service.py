"""服务装配：把存储、孪生、遥测、升级组装到一起并做重启恢复。"""
from __future__ import annotations

from pathlib import Path

from .ota import OtaManager
from .storage import Storage
from .telemetry import TelemetryIngestor
from .twin import TwinManager
from .util import now_ms

DEFAULT_DATA_DIR = "data"


class Services:
    """应用级依赖容器。"""

    def __init__(self, data_dir: str | Path = DEFAULT_DATA_DIR) -> None:
        self.data_dir = Path(data_dir)
        self.storage = Storage(self.data_dir)
        self.twins = TwinManager(self.storage)
        self.ingestor = TelemetryIngestor(self.storage, self.twins)
        self.ota = OtaManager(self.storage, self.twins)

    def recover(self, now: int | None = None) -> None:
        """重启恢复：注册信息与遥测重放重建孪生，升级状态从快照加载。"""
        current = now if now is not None else now_ms()
        self.twins.recover(current)
        self.ota.recover()
