"""HTTP 端到端：遥测接入 -> 孪生可见 -> 升级下发。"""
import os
import tempfile

os.environ["IOT_DB_PATH"] = os.path.join(tempfile.mkdtemp(), "api_test.db")

from fastapi.testclient import TestClient  # noqa: E402

from main import app  # noqa: E402


def test_http_end_to_end():
    with TestClient(app) as client:
        # 遥测上行
        r = client.post("/telemetry", json={
            "device_id": "dev-1", "seq": 1, "schema_version": 1,
            "payload": {"temp": 22},
        })
        assert r.status_code == 200 and r.json()["result"] == "applied"

        # 非法负载可区分
        r = client.post("/telemetry", json={"device_id": "dev-1", "seq": 2})
        assert r.status_code == 422 and r.json()["reason"] == "missing_field"

        # 孪生查询
        r = client.get("/twins/dev-1")
        assert r.json()["reported"] == {"temp": 22}

        # desired 更新与冲突
        r = client.patch("/twins/dev-1/desired",
                         json={"patch": {"target": 25}, "expected_version": 0})
        assert r.json()["desired_version"] == 1
        r = client.patch("/twins/dev-1/desired",
                         json={"patch": {"target": 26}, "expected_version": 0})
        assert r.status_code == 409

        # 升级批次与下发
        r = client.post("/upgrades/batches", json={
            "batch_id": "b1", "to_version": "2.0.0",
            "min_from_version": "1.0.0", "max_failure_ratio": 0.5,
        })
        assert r.status_code == 200
        r = client.post("/upgrades/batches/b1/dispatch",
                        json={"device_id": "dev-1", "from_version": "1.2.0"})
        task_id = r.json()["task_id"]
        # 重复下发被拒
        r = client.post("/upgrades/batches/b1/dispatch",
                        json={"device_id": "dev-1", "from_version": "1.2.0"})
        assert r.status_code == 409
        r = client.post(f"/upgrades/tasks/{task_id}/progress",
                        json={"status": "downloading"})
        assert r.json()["status"] == "downloading"
        print(f"[判定依据] 端到端链路通过，任务 {task_id[:8]} 状态=downloading")

        # 暂停/恢复可通过 HTTP 操作，pause_reason 明确区分手动暂停与熔断
        r = client.post("/upgrades/batches/b1/pause")
        assert r.status_code == 200
        assert r.json()["status"] == "paused"
        assert r.json()["pause_reason"] == "manual"
        r = client.post("/upgrades/batches/b1/dispatch",
                        json={"device_id": "dev-2", "from_version": "1.0.0"})
        assert r.status_code == 409  # 暂停期间停止下发
        r = client.post("/upgrades/batches/b1/resume")
        assert r.json()["status"] == "running" and r.json()["pause_reason"] is None

        # 批次状态可直接查询，不靠下发报错去猜
        r = client.get("/upgrades/batches/b1")
        assert r.status_code == 200 and r.json()["status"] == "running"
        r = client.get("/upgrades/batches/b-nope")
        assert r.status_code == 404

        # 触发熔断：再下发两台并都失败（2/3 > 0.5），批次自动停掉
        for dev in ("dev-2", "dev-3"):
            r = client.post("/upgrades/batches/b1/dispatch",
                            json={"device_id": dev, "from_version": "1.0.0"})
            tid = r.json()["task_id"]
            r = client.post(f"/upgrades/tasks/{tid}/progress",
                            json={"status": "failed"})
            assert r.status_code == 200
        r = client.get("/upgrades/batches/b1")
        assert r.json()["status"] == "paused"
        assert r.json()["pause_reason"] == "circuit_breaker"  # 熔断，可区分
        print("[判定依据] GET /upgrades/batches/b1：手动暂停=pause_reason manual，"
              "失败率 2/3 熔断后=paused/circuit_breaker，均可直接查询")

        # 带优先级的批次
        r = client.post("/upgrades/batches", json={
            "batch_id": "b2", "to_version": "3.0.0",
            "min_from_version": "1.0.0", "max_failure_ratio": 0.5, "priority": 7,
        })
        assert r.status_code == 200 and r.json()["priority"] == 7
