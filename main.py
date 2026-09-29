"""HTTP 入口：设备遥测接入、孪生查询/更新、升级下发。"""
from __future__ import annotations

import os
from contextlib import asynccontextmanager
from dataclasses import asdict

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.models import IngestResult, UpgradeStatus
from app.storage import Storage
from app.telemetry import TelemetryService
from app.twin import ConflictError, TwinService
from app.upgrade import UpgradeError, UpgradeService

DB_PATH = os.environ.get("IOT_DB_PATH", "iot.db")

storage = Storage(DB_PATH)
twin_service = TwinService(storage)
telemetry_service = TelemetryService(storage, on_applied=twin_service.apply_reported)
upgrade_service = UpgradeService(storage)


@asynccontextmanager
async def lifespan(app: FastAPI):
    twin_service.recover()  # 重启后从本地持久化数据重建孪生
    yield
    storage.close()


app = FastAPI(title="IoT Device Twin", lifespan=lifespan)


@app.get("/")
def read_root():
    return {"service": "iot-device-twin", "status": "ok"}


# ---- 遥测 ----
@app.post("/telemetry")
async def ingest_telemetry(request: Request):
    result, reason = telemetry_service.ingest(await request.body())
    status_code = 200 if result is not IngestResult.REJECTED else 422
    body = {"result": result.value}
    if reason is not None:
        body["reason"] = reason.value
    return JSONResponse(body, status_code=status_code)


# ---- 孪生 ----
@app.get("/twins/{device_id}")
def get_twin(device_id: str):
    twin = twin_service.get(device_id)
    if twin is None:
        return JSONResponse({"error": "unknown_device"}, status_code=404)
    return asdict(twin)


@app.patch("/twins/{device_id}/desired")
async def patch_desired(device_id: str, request: Request):
    body = await request.json()
    try:
        twin = twin_service.update_desired(
            device_id, body.get("patch", {}), body.get("expected_version")
        )
    except ConflictError as e:
        return JSONResponse({"error": "version_conflict", "detail": str(e)}, status_code=409)
    return {"desired_version": twin.desired_version, "desired": twin.desired}


# ---- 升级 ----
@app.post("/upgrades/batches")
async def create_batch(request: Request):
    body = await request.json()
    try:
        batch = upgrade_service.create_batch(
            body["batch_id"], body["to_version"],
            body["min_from_version"], body["max_failure_ratio"],
            priority=int(body.get("priority", 0)),
        )
    except (UpgradeError, ValueError, TypeError) as e:
        return JSONResponse({"error": str(e)}, status_code=409)
    return asdict(batch) | {"status": batch.status.value}


@app.get("/upgrades/batches/{batch_id}")
def get_batch(batch_id: str):
    batch = storage.load_batch(batch_id)
    if batch is None:
        return JSONResponse({"error": "unknown_batch"}, status_code=404)
    return (
        asdict(batch)
        | {"status": batch.status.value}
        | {"pause_reason": batch.pause_reason.value if batch.pause_reason else None}
    )


@app.post("/upgrades/batches/{batch_id}/enroll")
async def enroll_device(batch_id: str, request: Request):
    body = await request.json()
    try:
        upgrade_service.enroll(batch_id, body["device_id"])
    except UpgradeError as e:
        return JSONResponse({"error": str(e)}, status_code=409)
    return {"batch_id": batch_id, "device_id": body["device_id"], "enrolled": True}


@app.post("/upgrades/batches/{batch_id}/pause")
def pause_batch(batch_id: str):
    try:
        batch = upgrade_service.pause_batch(batch_id)
    except UpgradeError as e:
        return JSONResponse({"error": str(e)}, status_code=409)
    return (
        asdict(batch)
        | {"status": batch.status.value}
        | {"pause_reason": batch.pause_reason.value if batch.pause_reason else None}
    )


@app.post("/upgrades/batches/{batch_id}/resume")
def resume_batch(batch_id: str):
    try:
        batch = upgrade_service.resume_batch(batch_id)
    except UpgradeError as e:
        return JSONResponse({"error": str(e)}, status_code=409)
    return (
        asdict(batch)
        | {"status": batch.status.value}
        | {"pause_reason": batch.pause_reason.value if batch.pause_reason else None}
    )


@app.post("/upgrades/batches/{batch_id}/dispatch")
async def dispatch(batch_id: str, request: Request):
    body = await request.json()
    try:
        task = upgrade_service.dispatch(batch_id, body["device_id"], body["from_version"])
    except UpgradeError as e:
        return JSONResponse({"error": str(e)}, status_code=409)
    return asdict(task) | {"status": task.status.value}


@app.post("/upgrades/tasks/{task_id}/progress")
async def report_progress(task_id: str, request: Request):
    body = await request.json()
    try:
        task = upgrade_service.report_progress(task_id, UpgradeStatus(body["status"]))
    except (UpgradeError, ValueError) as e:
        return JSONResponse({"error": str(e)}, status_code=409)
    return asdict(task) | {"status": task.status.value}


@app.post("/upgrades/check-timeouts")
async def check_timeouts(request: Request):
    body = await request.json()
    timed_out = upgrade_service.check_timeouts(body["now"], body["timeout_seconds"])
    return {"timed_out": [t.task_id for t in timed_out]}
