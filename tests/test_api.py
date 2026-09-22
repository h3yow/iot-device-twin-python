"""HTTP 端到端：注册 -> 遥测 -> 孪生 -> 升级批次 -> 状态机 -> 重启恢复。"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def test_full_flow_over_http(client, data_dir):
    # 1) 注册设备与版本遥测
    assert client.post("/devices/d1").json()["ok"]
    for ts, seq, temp, fw in [(100, 1, 20.0, "1.0.0"), (110, 2, 21.0, "1.0.0")]:
        r = client.post("/devices/d1/telemetry",
                        json={"ts": ts, "seq": seq, "data": {"temp": temp, "fw_version": fw}})
        assert r.status_code == 200, r.json()
    # 重复与乱序
    r = client.post("/devices/d1/telemetry",
                    json={"ts": 110, "seq": 2, "data": {"temp": 21.0, "fw_version": "1.0.0"}})
    assert r.json()["outcome"] == "duplicate"
    r = client.post("/devices/d1/telemetry",
                    json={"ts": 105, "seq": 9, "data": {"temp": 99, "fw_version": "1.0.0"}})
    assert r.json()["outcome"] == "late"
    twin = client.get("/devices/d1/twin").json()["twin"]
    logger.info("HTTP 遥测收敛：reported=%s version=%s", twin["reported"], twin["version"])
    assert twin["reported"]["temp"] == 21.0

    # 2) OCC：陈旧版本被拒
    assert client.patch("/devices/d1/twin",
                        json={"reported": {"label": "shop-a"},
                              "expected_version": 999}).status_code == 409
    r = client.patch("/devices/d1/twin",
                     json={"reported": {"label": "shop-a"},
                           "expected_version": twin["version"]})
    assert r.status_code == 200

    # 3) 升级批次 + 门限 + 推进
    client.post("/devices/old"); 
    client.post("/devices/d2")
    client.post("/devices/d2/telemetry",
                json={"ts": 1, "seq": 1, "data": {"fw_version": "1.2.0"}})
    client.post("/devices/old/telemetry",
                json={"ts": 1, "seq": 1, "data": {"fw_version": "0.5.0"}})
    r = client.post("/ota/batches", json={
        "batch_id": "B1", "to_version": "2.0.0", "min_current_version": "1.0.0",
        "max_concurrent": 1, "device_ids": ["d1", "d2", "old"]})
    assert r.status_code == 200, r.json()
    r = client.post("/ota/batches/B1/dispatch").json()
    logger.info("HTTP 推进结果 started=%s skipped=%s",
                [t["device_id"] for t in r["started"]], r["skipped"])
    assert [t["device_id"] for t in r["started"]] == ["d1"]
    assert {s["device_id"]: s["reason"] for s in r["skipped"]}["old"] == "below_version_gate"

    # 4) 成功上报后槽位释放；重复上报被拒
    assert client.post("/ota/tasks/B1:d1/status",
                       json={"status": "success"}).json()["ok"]
    dup = client.post("/ota/tasks/B1:d1/status", json={"status": "success"})
    assert dup.status_code == 422 and dup.json()["error"] == "already_terminal"
    r = client.post("/ota/batches/B1/dispatch").json()
    assert [t["device_id"] for t in r["started"]] == ["d2"]

    # 5) 超时扫描
    r = client.post("/ota/sweep", json={"timeout_ms": 0}).json()
    assert [t["device_id"] for t in r["timed_out"]] == ["d2"]
    logger.info("超时扫描结果=%s", [t["task_id"] for t in r["timed_out"]])

    # 6) 重启后 API 状态一致
    from fastapi.testclient import TestClient
    from app.api import create_app
    from app.service import Services
    restarted = Services(data_dir)
    restarted.recover()
    c2 = TestClient(create_app(restarted))
    twin2 = c2.get("/devices/d1/twin").json()["twin"]
    assert twin2["reported"]["label"] == "shop-a"
    assert twin2["reported"]["temp"] == 21.0
    r = c2.post("/ota/batches/B1/dispatch").json()
    assert {s["device_id"]: s["reason"] for s in r["skipped"]}["d2"] == "already_timed_out"
    logger.info("HTTP 重启后孪生与升级状态一致重建")
