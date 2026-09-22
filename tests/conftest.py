"""公共夹具：每个用例独立的数据目录与已恢复的服务。"""
from __future__ import annotations

import logging
import tempfile
from pathlib import Path

import pytest

from app.service import Services

# 静默测试客户端的 HTTP 访问日志，突出单测中打印的关键输入与判定依据
logging.getLogger("httpx").setLevel(logging.WARNING)


@pytest.fixture
def data_dir() -> Path:
    with tempfile.TemporaryDirectory() as tmp:
        yield Path(tmp)


@pytest.fixture
def svc(data_dir: Path) -> Services:
    services = Services(data_dir)
    services.recover(now=1_000_000)
    return services


@pytest.fixture
def client(svc: Services):
    from fastapi.testclient import TestClient
    from app.api import create_app
    return TestClient(create_app(svc))


def register(svc: Services, *device_ids: str) -> None:
    for did in device_ids:
        svc.storage.register_device(did)


def report_fw(svc: Services, device_id: str, fw: str, ts: int = 1, seq: int = 1) -> None:
    from app.models import TelemetryEvent
    svc.twins.admit_and_persist(
        TelemetryEvent(device_id, ts, seq, {"fw_version": fw}, ts))
