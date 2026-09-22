"""孪生：并发写入无陈旧覆盖、OCC 冲突可区分、重启状态与在线判定一致重建。"""
from __future__ import annotations

import logging
import threading

import pytest

from app.models import TelemetryEvent
from app.twin import ConflictError
from app.util import now_ms

from .conftest import register
from app.twin import ONLINE_TTL_MS

logger = logging.getLogger(__name__)


def test_concurrent_updates_no_stale_overwrite(svc):
    register(svc, "dev1")
    svc.twins.update_reported("dev1", {"init": True})
    start = threading.Barrier(8)
    conflicts: list[str] = []

    def worker(i: int):
        start.wait()
        # 乐观重试循环：基于最新版本号重试，直到成功
        for _ in range(100):
            current = svc.twins.get("dev1")
            try:
                svc.twins.update_reported(
                    "dev1", {f"k{i}": i}, expected_version=current.version)
                logger.info("worker%s 基于版本 %s 写入成功", i, current.version)
                return
            except ConflictError as exc:
                conflicts.append(str(exc))
        raise AssertionError(f"worker {i} failed to converge")

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    twin = svc.twins.get("dev1")
    logger.info("并发收敛 reported=%s version=%s 冲突次数=%d",
                twin.reported, twin.version, len(conflicts))
    # 8 个键全部合并，无写入被陈旧覆盖丢失；初始 8 次中有并发冲突发生
    assert all(f"k{i}" in twin.reported for i in range(8))
    assert twin.reported["init"] is True
    assert twin.version == 9  # 初始 1 次 + 恰好 8 次成功
    assert len(conflicts) > 0


def test_stale_write_is_rejected_not_applied(svc):
    register(svc, "dev1")
    svc.twins.update_reported("dev1", {"a": 1})  # version 1
    with pytest.raises(ConflictError) as ei:
        svc.twins.update_reported("dev1", {"a": 999}, expected_version=0)
    logger.info("陈旧写入被拒绝依据：%s", ei.value)
    twin = svc.twins.get("dev1")
    assert twin.version == 1 and twin.reported == {"a": 1}


def test_telemetry_and_patch_share_device_lock(svc):
    """遥测应用与 PATCH 共用每设备锁：最终版本严格单调，无丢更新。"""
    register(svc, "dev1")
    errors = []

    def feed_telemetry():
        for seq in range(1, 51):
            try:
                evt = TelemetryEvent("dev1", 1000 + seq, seq,
                                     {"temp": seq}, 2000 + seq)
                with svc.twins.device_lock("dev1"):
                    svc.twins.admit_and_persist(evt)
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

    def patch_twin():
        for i in range(50):
            for _ in range(100):
                v = svc.twins.get("dev1").version
                try:
                    svc.twins.update_reported("dev1", {f"p{i}": i},
                                              expected_version=v)
                    break
                except ConflictError:
                    continue

    t1 = threading.Thread(target=feed_telemetry)
    t2 = threading.Thread(target=patch_twin)
    t1.start(); t2.start(); t1.join(); t2.join()
    assert not errors
    twin = svc.twins.get("dev1")
    logger.info("混合并发后 version=%s，遥测键 temp=%s，补丁键数量=%d",
                twin.version, twin.reported.get("temp"),
                sum(1 for k in twin.reported if k.startswith("p")))
    assert twin.reported["temp"] == 50
    assert sum(1 for k in twin.reported if k.startswith("p")) == 50
    assert twin.version == 100


def test_restart_rebuilds_state_and_online_status(svc, data_dir):
    register(svc, "dev-online", "dev-offline")
    base = now_ms()
    svc.twins.admit_and_persist(
        TelemetryEvent("dev-online", 100, 1, {"temp": 1}, base))
    svc.twins.admit_and_persist(
        TelemetryEvent("dev-offline", 100, 1, {"temp": 2}, base - ONLINE_TTL_MS - 1))

    from app.service import Services
    restarted = Services(data_dir)
    restarted.recover(now=base + 1_000)
    online = restarted.twins.get("dev-online", base + 1_000)
    offline = restarted.twins.get("dev-offline", base + 1_000)
    logger.info("重启重建在线判定：dev-online online=%s last_seen 差 %dms；"
                "dev-offline online=%s last_seen 差 %dms（TTL=%dms）",
                online.online, base + 1_000 - online.last_seen,
                offline.online, base + 1_000 - offline.last_seen, ONLINE_TTL_MS)
    assert online.online is True
    assert offline.online is False
    assert online.reported == {"temp": 1}
