"""遥测接入：重复、乱序、迟到收敛与可区分的拒绝原因。"""
from __future__ import annotations

import logging

from app.models import TelemetryEvent, TelemetryOutcome, TelemetryRejectReason

from .conftest import register

logger = logging.getLogger(__name__)


def _post(svc, device_id, raw):
    result = svc.ingestor.ingest(device_id, raw)
    logger.info("遥测输入 device=%s raw=%.200r -> ok=%s outcome=%s reason=%s detail=%s",
                device_id, raw, result.ok,
                result.outcome.value if result.outcome else None,
                result.reject_reason.value if result.reject_reason else None,
                result.detail)
    return result


def test_duplicate_out_of_order_and_late_converge(svc):
    register(svc, "dev1")
    # 输入序列：正常 -> 乱序旧事件 -> 重复 -> 正常更新
    seq_inputs = [
        (100, 1, {"temp": 20.0}),
        (90, 0, {"temp": 99.0}),    # 乱序到达：应持久化但不覆盖
        (100, 1, {"temp": 20.0}),   # 重复：幂等忽略
        (110, 2, {"temp": 21.0}),   # 最新：应覆盖
    ]
    outcomes = []
    for ts, seq, data in seq_inputs:
        r = _post(svc, "dev1", {"ts": ts, "seq": seq, "data": data})
        outcomes.append(r.outcome)

    logger.info("判定结果序列=%s，期望 [applied, late, duplicate, applied]",
                [o.value for o in outcomes])
    assert outcomes == [
        TelemetryOutcome.APPLIED,
        TelemetryOutcome.LATE_OUT_OF_ORDER,
        TelemetryOutcome.DUPLICATE,
        TelemetryOutcome.APPLIED,
    ]
    twin = svc.twins.get("dev1")
    logger.info("收敛后孪生 reported=%r version=%s last_ts=%s last_seq=%s",
                twin.reported, twin.version, twin.last_ts, twin.last_seq)
    assert twin.reported == {"temp": 21.0}
    assert (twin.last_ts, twin.last_seq) == (110, 2)
    assert twin.version == 2  # 仅 applied 推进版本
    # 乱序事件确实落了日志（可审计/可重放）
    assert len(svc.storage.load_telemetry()) == 3  # 重复事件不落日志


def test_late_after_restart_still_rejected(svc, data_dir):
    register(svc, "dev1")
    _post(svc, "dev1", {"ts": 100, "seq": 1, "data": {"temp": 1}})
    _post(svc, "dev1", {"ts": 110, "seq": 2, "data": {"temp": 2}})

    # 模拟服务重启：全新容器从同一目录恢复
    from app.service import Services
    restarted = Services(data_dir)
    restarted.recover(now=2_000_000)
    r1 = _post(restarted, "dev1", {"ts": 110, "seq": 2, "data": {"temp": 2}})
    r2 = _post(restarted, "dev1", {"ts": 105, "seq": 9, "data": {"temp": 50}})
    logger.info("重启后水位判定：重复=%s 迟到=%s", r1.outcome.value, r2.outcome.value)
    assert r1.outcome is TelemetryOutcome.DUPLICATE
    assert r2.outcome is TelemetryOutcome.LATE_OUT_OF_ORDER
    assert restarted.twins.get("dev1").reported == {"temp": 2}


def test_reject_reasons_are_distinguishable(svc):
    register(svc, "dev1")
    cases = [
        ("{not json", TelemetryRejectReason.UNPARSEABLE_PAYLOAD),
        (b"\xff\xfe", TelemetryRejectReason.UNPARSEABLE_PAYLOAD),
        ({"seq": 0, "data": {}}, TelemetryRejectReason.MISSING_FIELD),
        ({"ts": 1, "data": {}}, TelemetryRejectReason.MISSING_FIELD),
        ({"ts": "100", "seq": 0, "data": {}}, TelemetryRejectReason.INCOMPATIBLE_FIELD_TYPE),
        ({"ts": 100, "seq": -1, "data": {}}, TelemetryRejectReason.INCOMPATIBLE_FIELD_TYPE),
        ({"ts": 100, "seq": 0, "data": {"k": {"nested": 1}}},
         TelemetryRejectReason.INCOMPATIBLE_FIELD_TYPE),
        ({"ts": 100, "seq": 0, "data": {"k": [1, 2]}},
         TelemetryRejectReason.INCOMPATIBLE_FIELD_TYPE),
    ]
    for raw, expected in cases:
        r = _post(svc, "dev1", raw)
        assert not r.ok and r.reject_reason is expected, (raw, r)
    r = _post(svc, "ghost", {"ts": 1, "seq": 0, "data": {}})
    logger.info("未注册设备判定 reason=%s", r.reject_reason.value)
    assert r.reject_reason is TelemetryRejectReason.UNKNOWN_DEVICE


def test_same_ts_ordering_by_seq(svc):
    register(svc, "dev1")
    _post(svc, "dev1", {"ts": 100, "seq": 5, "data": {"v": "a"}})
    r_late = _post(svc, "dev1", {"ts": 100, "seq": 3, "data": {"v": "b"}})
    r_new = _post(svc, "dev1", {"ts": 100, "seq": 8, "data": {"v": "c"}})
    logger.info("同 ts 不同 seq：旧序号=%s 新序号=%s",
                r_late.outcome.value, r_new.outcome.value)
    assert r_late.outcome is TelemetryOutcome.LATE_OUT_OF_ORDER
    assert r_new.outcome is TelemetryOutcome.APPLIED
    assert svc.twins.get("dev1").reported == {"v": "c"}
