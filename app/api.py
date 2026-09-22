"""HTTP 路由。

响应约定：
- 成功 {"ok": true, ...}，失败 {"ok": false, "error": "<code>", "detail": "..."}
- 遥测失败按原因返回不同状态码：400 解析失败 / 404 设备未知 / 422 字段问题
- 孪生版本冲突：409 error=version_conflict
- 升级规则违反：422，error 为具体错误码（already_terminal / illegal_transition 等）
"""
from __future__ import annotations

import json
from dataclasses import asdict
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .models import OtaStatus, TelemetryRejectReason
from .ota import OtaError
from .service import Services
from .twin import ConflictError
from .util import now_ms

_REJECT_STATUS = {
    TelemetryRejectReason.UNPARSEABLE_PAYLOAD: 400,
    TelemetryRejectReason.UNKNOWN_DEVICE: 404,
    TelemetryRejectReason.MISSING_FIELD: 422,
    TelemetryRejectReason.INCOMPATIBLE_FIELD_TYPE: 422,
}


def _err(code: str, detail: str, status: int) -> JSONResponse:
    return JSONResponse({"ok": False, "error": code, "detail": detail},
                        status_code=status)


def create_app(services: Services | None = None) -> FastAPI:
    app = FastAPI(title="iot-device-twin")
    svc = services or Services()
    app.state.svc = svc

    @app.post("/devices/{device_id}")
    async def register_device(device_id: str) -> JSONResponse:
        svc.storage.register_device(device_id)
        return JSONResponse({"ok": True, "device_id": device_id})

    @app.post("/devices/{device_id}/telemetry")
    async def ingest_telemetry(device_id: str, request: Request) -> JSONResponse:
        raw = await request.body()
        result = svc.ingestor.ingest(device_id, raw)
        if not result.ok:
            assert result.reject_reason is not None
            return _err(result.reject_reason.value, result.detail,
                        _REJECT_STATUS[result.reject_reason])
        assert result.outcome is not None and result.event is not None
        return JSONResponse({
            "ok": True,
            "outcome": result.outcome.value,
            "device_id": device_id,
            "ts": result.event.ts,
            "seq": result.event.seq,
            "detail": result.detail,
        })

    @app.get("/devices/{device_id}/twin")
    async def get_twin(device_id: str) -> JSONResponse:
        twin = svc.twins.get(device_id, now_ms())
        if twin is None:
            return _err("device_not_found", f"device '{device_id}' has no twin", 404)
        return JSONResponse({"ok": True, "twin": asdict(twin)})

    @app.patch("/devices/{device_id}/twin")
    async def patch_twin(device_id: str, request: Request) -> JSONResponse:
        try:
            body = json.loads(await request.body())
        except ValueError as exc:
            return _err("unparseable_payload", f"invalid json: {exc}", 400)
        if not isinstance(body, dict) or "reported" not in body:
            return _err("missing_field", "body must be an object with 'reported'", 422)
        patch = body["reported"]
        if not isinstance(patch, dict):
            return _err("incompatible_field_type", "'reported' must be an object", 422)
        expected = body.get("expected_version")
        if expected is not None and not isinstance(expected, int):
            return _err("incompatible_field_type",
                        "'expected_version' must be an integer", 422)
        try:
            twin = svc.twins.update_reported(device_id, patch, expected)
        except ConflictError as exc:
            return _err("version_conflict", str(exc), 409)
        return JSONResponse({"ok": True, "twin": asdict(twin)})

    @app.post("/ota/batches")
    async def create_batch(request: Request) -> JSONResponse:
        try:
            body = json.loads(await request.body())
        except ValueError as exc:
            return _err("unparseable_payload", f"invalid json: {exc}", 400)
        required = ("batch_id", "to_version", "min_current_version",
                    "max_concurrent", "device_ids")
        missing = [f for f in required if f not in body]
        if missing:
            return _err("missing_field", f"missing fields: {missing}", 422)
        try:
            batch = svc.ota.create_batch(
                body["batch_id"], body["to_version"],
                body["min_current_version"], int(body["max_concurrent"]),
                list(body["device_ids"]))
        except OtaError as exc:
            return _err(exc.code, str(exc), 422)
        return JSONResponse({"ok": True, "batch": asdict(batch)})

    @app.post("/ota/batches/{batch_id}/dispatch")
    async def dispatch_batch(batch_id: str) -> JSONResponse:
        try:
            result = svc.ota.dispatch(batch_id, now_ms())
        except OtaError as exc:
            return _err(exc.code, str(exc), 404)
        return JSONResponse({
            "ok": True,
            "started": [asdict_with_status(t) for t in result.started],
            "skipped": [{"device_id": d, "reason": r} for d, r in result.skipped],
        })

    @app.post("/ota/tasks/{task_id}/status")
    async def task_status(task_id: str, request: Request) -> JSONResponse:
        try:
            body = json.loads(await request.body())
        except ValueError as exc:
            return _err("unparseable_payload", f"invalid json: {exc}", 400)
        try:
            status = OtaStatus(body.get("status"))
        except ValueError:
            allowed = [s.value for s in OtaStatus]
            return _err("incompatible_field_type",
                        f"status must be one of {allowed}", 422)
        try:
            task = svc.ota.report_status(task_id, status, now_ms())
        except OtaError as exc:
            code_status = 404 if exc.code == "task_not_found" else 422
            return _err(exc.code, str(exc), code_status)
        return JSONResponse({"ok": True, "task": asdict_with_status(task)})

    @app.post("/ota/sweep")
    async def sweep(request: Request) -> JSONResponse:
        try:
            body = json.loads(await request.body())
            timeout_ms = int(body.get("timeout_ms", 60_000))
        except ValueError as exc:
            return _err("unparseable_payload", f"invalid json: {exc}", 400)
        timed_out = svc.ota.sweep_timeouts(now_ms(), timeout_ms)
        return JSONResponse({"ok": True,
                             "timed_out": [asdict_with_status(t) for t in timed_out]})

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"ok": "true", "service": "iot-device-twin"}

    return app


def asdict_with_status(task: Any) -> dict:
    data = asdict(task)
    data["status"] = task.status.value
    return data
