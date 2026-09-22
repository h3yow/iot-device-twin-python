"""遥测接入：重复上报、乱序到达、超时迟到、非法负载。"""
import json

import pytest

from app.models import IngestResult, TelemetryRejectReason
from app.storage import Storage
from app.telemetry import TelemetryService
from app.twin import TwinService


@pytest.fixture()
def services():
    storage = Storage()
    twin = TwinService(storage)
    svc = TelemetryService(storage, on_applied=twin.apply_reported)
    twin.recover()
    yield svc, twin
    storage.close()


def msg(device="d1", seq=1, ver=1, payload=None):
    return json.dumps(
        {"device_id": device, "seq": seq, "schema_version": ver,
         "payload": payload or {"temp": 20}}
    ).encode()


def test_duplicate_report_is_idempotent(services):
    svc, twin = services
    body = msg(seq=1, payload={"temp": 21})
    r1, _ = svc.ingest(body)
    r2, _ = svc.ingest(body)
    print(f"[判定依据] 首次={r1.value} 重复={r2.value}; "
          f"孪生 reported={twin.get('d1').reported}")
    assert r1 is IngestResult.APPLIED
    assert r2 is IngestResult.DUPLICATE
    assert twin.get("d1").reported == {"temp": 21}
    assert twin.get("d1").reported_version == 1


def test_out_of_order_and_late_arrival_do_not_regress_state(services):
    svc, twin = services
    # 乱序：seq 3 先于 seq 2 到达；超时迟到：seq 1 最后才到
    results = [svc.ingest(msg(seq=s, payload={"temp": 20 + s}))[0] for s in (3, 2, 1)]
    state = twin.get("d1")
    print(f"[判定依据] 到达顺序 seq=(3,2,1) 结果={[r.value for r in results]}; "
          f"收敛 reported={state.reported} version={state.reported_version}")
    assert results == [IngestResult.APPLIED, IngestResult.STALE, IngestResult.STALE]
    assert state.reported == {"temp": 23}  # 最新状态不被旧数据覆盖
    assert state.reported_version == 3


def test_reject_reasons_are_distinguishable(services):
    svc, _ = services
    cases = [
        (b"\x00\x01not-json", TelemetryRejectReason.INVALID_JSON),
        (msg(ver=99), TelemetryRejectReason.INVALID_VERSION),
        (json.dumps({"device_id": "d1", "seq": 1}).encode(),
         TelemetryRejectReason.MISSING_FIELD),
        (json.dumps({"device_id": "d1", "seq": "abc", "schema_version": 1,
                     "payload": {}}).encode(), TelemetryRejectReason.INVALID_TYPE),
    ]
    for body, expected in cases:
        result, reason = svc.ingest(body)
        print(f"[判定依据] 输入={body[:60]!r} -> result={result.value} reason={reason.value}")
        assert result is IngestResult.REJECTED
        assert reason is expected
