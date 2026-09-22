"""升级：状态机、单设备互斥、批次上限、版本门限、失败/超时中断与重复下发。"""
from __future__ import annotations

import logging

import pytest

from app.models import OtaStatus
from app.ota import OtaError

from .conftest import register, report_fw

logger = logging.getLogger(__name__)


def _new_batch(svc, devices, batch_id="B1", to_version="2.0.0",
               gate="1.0.0", concurrency=2):
    for did, fw in devices.items():
        register(svc, did)
        report_fw(svc, did, fw)
    return svc.ota.create_batch(batch_id, to_version, gate, concurrency,
                                list(devices))


def test_batch_constraints_and_version_gate(svc):
    devices = {"ok-1.0": "1.0.0", "ok-1.5": "1.5.0", "old-0.9": "0.9.0"}
    _new_batch(svc, devices, concurrency=2)

    result = svc.ota.dispatch("B1", now_ms=1000)
    started = sorted(t.device_id for t in result.started)
    skipped = dict(result.skipped)
    logger.info("首次推进：启动=%s，跳过及依据=%s", started, skipped)
    assert started == ["ok-1.0", "ok-1.5"]           # 并发上限=2
    assert skipped["old-0.9"] == "below_version_gate"  # 门限不可绕过

    # 槽位未释放前，门限设备即便升级了版本，本批次仍需先等槽位
    result2 = svc.ota.dispatch("B1", now_ms=1001)
    assert result2.started == []
    assert result2.skipped_reason("ok-1.0") == "already_dispatched"
    logger.info("重复推进判定：%s", result2.skipped)


def test_duplicate_dispatch_is_idempotent(svc):
    _new_batch(svc, {"d1": "1.0.0"}, concurrency=5)
    r1 = svc.ota.dispatch("B1", 1000)
    r2 = svc.ota.dispatch("B1", 1001)
    assert len(r1.started) == 1 and r2.started == []
    assert r2.skipped_reason("d1") == "already_dispatched"
    # 成功后再次推进仍不会创建第二个任务
    svc.ota.report_status("B1:d1", OtaStatus.SUCCESS, 2000)
    r3 = svc.ota.dispatch("B1", 2001)
    assert r3.started == []
    assert r3.skipped_reason("d1") == "already_success"
    # 重复上报终态被拒绝，原因可区分
    with pytest.raises(OtaError) as ei:
        svc.ota.report_status("B1:d1", OtaStatus.SUCCESS, 2002)
    logger.info("重复终态上报 code=%s", ei.value.code)
    assert ei.value.code == "already_terminal"


def test_one_device_cannot_have_two_active_tasks(svc):
    _new_batch(svc, {"d1": "1.0.0"}, batch_id="B1", concurrency=5)
    svc.ota.dispatch("B1", 1000)
    # 另一个批次想对同一进行中设备下发
    svc.ota.create_batch("B2", "3.0.0", "1.0.0", 5, ["d1"])
    r = svc.ota.dispatch("B2", 1001)
    assert r.started == []
    assert r.skipped_reason("d1") == "device_busy"
    logger.info("跨批次重复下发判定 device_busy，活动任务=%s",
                svc.ota.active_task_of("d1").task_id)
    with pytest.raises(OtaError) as ei:
        svc.ota.report_status("B2:d1", OtaStatus.SUCCESS, 1)
    assert ei.value.code == "task_not_found"


def test_failed_device_is_isolated(svc):
    _new_batch(svc, {"d1": "1.0.0", "d2": "1.0.0"}, concurrency=1)
    svc.ota.dispatch("B1", 1000)  # 仅 d1 启动
    svc.ota.report_status("B1:d1", OtaStatus.FAILED, 2000)
    r = svc.ota.dispatch("B1", 2001)
    logger.info("失败后推进：新启动=%s，d1 判定=%s",
                [t.device_id for t in r.started], r.skipped_reason("d1"))
    assert r.skipped_reason("d1") == "already_failed"
    assert [t.device_id for t in r.started] == ["d2"]  # 其他设备不受影响


def test_timeout_sweep_stops_progress(svc):
    _new_batch(svc, {"d1": "1.0.0", "d2": "1.0.0"}, concurrency=1)
    svc.ota.dispatch("B1", 1000)
    # 未超时：sweep 不动
    assert svc.ota.sweep_timeouts(now_ms=2000, timeout_ms=60_000) == []
    # 超时后 d1 被标记 TIMED_OUT，且永远不再推进
    timed_out = svc.ota.sweep_timeouts(now_ms=9_999_999, timeout_ms=60_000)
    assert [t.task_id for t in timed_out] == ["B1:d1"]
    assert timed_out[0].status is OtaStatus.TIMED_OUT
    r = svc.ota.dispatch("B1", 10_000_000)
    assert r.skipped_reason("d1") == "already_timed_out"
    assert [t.device_id for t in r.started] == ["d2"]
    logger.info("超时设备隔离依据=%s，批次继续推进到=%s",
                r.skipped_reason("d1"), [t.device_id for t in r.started])


def test_illegal_transitions_rejected(svc):
    _new_batch(svc, {"d1": "1.0.0"}, concurrency=5)
    # PENDING 任务不存在（dispatch 即 IN_PROGRESS）；IN_PROGRESS 不能再 IN_PROGRESS
    svc.ota.dispatch("B1", 1000)
    with pytest.raises(OtaError) as ei:
        svc.ota.report_status("B1:d1", OtaStatus.PENDING, 1001)
    assert ei.value.code == "illegal_transition"
    svc.ota.report_status("B1:d1", OtaStatus.SUCCESS, 2000)
    with pytest.raises(OtaError) as ei:
        svc.ota.report_status("B1:d1", OtaStatus.FAILED, 2001)
    assert ei.value.code == "already_terminal"
    logger.info("非法迁移全部拒绝：illegal_transition / already_terminal")


def test_ota_state_survives_restart(svc, data_dir):
    _new_batch(svc, {"d1": "1.0.0", "d2": "1.0.0"}, concurrency=1)
    svc.ota.dispatch("B1", 1000)
    svc.ota.report_status("B1:d1", OtaStatus.SUCCESS, 2000)
    svc.ota.dispatch("B1", 2001)  # d1 成功后 d2 才被推进

    from app.service import Services
    restarted = Services(data_dir)
    restarted.recover(now=3000)
    assert restarted.ota.tasks["B1:d1"].status is OtaStatus.SUCCESS
    assert restarted.ota.tasks["B1:d2"].status is OtaStatus.IN_PROGRESS
    r = restarted.ota.dispatch("B1", 3001)
    assert r.skipped_reason("d1") == "already_success"
    assert r.skipped_reason("d2") == "already_dispatched"
    logger.info("升级状态重启恢复一致：d1=%s d2=%s",
                restarted.ota.tasks["B1:d1"].status.value,
                restarted.ota.tasks["B1:d2"].status.value)


def test_missing_fw_version_cannot_be_dispatched(svc):
    register(svc, "unknown-fw")
    svc.ota.create_batch("B1", "2.0.0", "1.0.0", 5, ["unknown-fw"])
    r = svc.ota.dispatch("B1", 1000)
    assert r.skipped_reason("unknown-fw") == "missing_fw_version"
    logger.info("缺少 fw_version 的设备被拦截：missing_fw_version")
