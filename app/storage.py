"""本地 JSON 文件持久化。

数据目录下包含：
- devices.json     设备注册表
- telemetry.jsonl  遥测事件追加日志
- twins.json       孪生状态快照
- ota.json         升级批次与任务

所有快照写入采用“临时文件 + os.replace”原子替换，
保证进程在写入中途被杀死后不会留下半文件。
"""
from __future__ import annotations

import json
import os
import tempfile
from dataclasses import asdict
from pathlib import Path

from .models import Batch, OtaStatus, TelemetryEvent, TwinState, UpgradeTask


class Storage:
    """本地文件存储，进程重启后可从中重建全部状态。"""

    def __init__(self, data_dir: str | Path) -> None:
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)

    # ---- 通用原子写 ----
    def _path(self, name: str) -> Path:
        return self.data_dir / name

    def _atomic_write_json(self, name: str, obj: object) -> None:
        target = self._path(name)
        fd, tmp = tempfile.mkstemp(dir=self.data_dir, suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(obj, f, ensure_ascii=False)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, target)
        except BaseException:
            os.unlink(tmp)
            raise

    def _read_json(self, name: str, default: object) -> object:
        path = self._path(name)
        if not path.exists():
            return default
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    # ---- 设备注册 ----
    def load_devices(self) -> set[str]:
        data = self._read_json("devices.json", {"devices": []})
        return set(data["devices"])  # type: ignore[index]

    def register_device(self, device_id: str) -> None:
        devices = self.load_devices()
        devices.add(device_id)
        self._atomic_write_json("devices.json", {"devices": sorted(devices)})

    # ---- 遥测事件：追加日志 ----
    def append_telemetry(self, event: TelemetryEvent) -> None:
        path = self._path("telemetry.jsonl")
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(asdict(event), ensure_ascii=False) + "\n")
            f.flush()
            os.fsync(f.fileno())

    def load_telemetry(self) -> list[TelemetryEvent]:
        path = self._path("telemetry.jsonl")
        if not path.exists():
            return []
        events: list[TelemetryEvent] = []
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    events.append(TelemetryEvent(**json.loads(line)))
        return events

    # ---- 孪生快照 ----
    def save_twin(self, twin: TwinState) -> None:
        twins = self.load_twins()
        twins[twin.device_id] = twin
        self._save_twins(twins)

    def save_twins(self, twins: dict[str, TwinState]) -> None:
        self._save_twins(twins)

    def _save_twins(self, twins: dict[str, TwinState]) -> None:
        self._atomic_write_json("twins.json", {
            did: asdict(t) for did, t in twins.items()
        })

    def load_twins(self) -> dict[str, TwinState]:
        data = self._read_json("twins.json", {})
        assert isinstance(data, dict)
        return {did: TwinState(**t) for did, t in data.items()}

    # ---- 升级批次与任务 ----
    def save_ota(self, batches: dict[str, Batch], tasks: dict[str, UpgradeTask]) -> None:
        self._atomic_write_json("ota.json", {
            "batches": {bid: asdict(b) for bid, b in batches.items()},
            "tasks": {tid: self._task_to_dict(t) for tid, t in tasks.items()},
        })

    @staticmethod
    def _task_to_dict(task: UpgradeTask) -> dict:
        d = asdict(task)
        d["status"] = task.status.value
        return d

    def load_ota(self) -> tuple[dict[str, Batch], dict[str, UpgradeTask]]:
        data = self._read_json("ota.json", {"batches": {}, "tasks": {}})
        assert isinstance(data, dict)
        batches = {
            bid: Batch(**bd) for bid, bd in data.get("batches", {}).items()
        }
        tasks = {
            tid: UpgradeTask(**{**td, "status": OtaStatus(td["status"])})
            for tid, td in data.get("tasks", {}).items()
        }
        return batches, tasks
