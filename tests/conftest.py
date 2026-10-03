"""pytest 输出组织：按业务域分组打印用例与判定结果。

不依赖 -s：用 pytest_runtest_logreport 钩子把每个用例的 PASS/FAIL/SKIP
和所属业务域稳定打印到终端，跑 `python -m pytest tests/ -v` 即可看到
"覆盖了哪些业务、每个业务的用例和结果"。
"""
from __future__ import annotations

# 业务模块 -> 中文业务域名称
_BUSINESS = {
    "test_telemetry": "遥测接入：乱序/重复/迟到收敛与四类拒绝原因",
    "test_twin": "设备孪生：并发 reported/desired 更新、乐观锁与重启恢复",
    "test_upgrade": "升级批次：版本门限/状态机/暂停恢复/失败熔断/优先级抢占与名额释放",
    "test_api": "HTTP 端到端：遥测-孪生-升级全链路接口",
}


def _domain(nodeid: str) -> str:
    module = nodeid.split("::", 1)[0].rsplit("/", 1)[-1].rsplit(".", 1)[0]
    return _BUSINESS.get(module, module)


def pytest_report_header(config):
    return ["业务域分组：" + "；".join(f"{k}={v}" for k, v in _BUSINESS.items())]


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    tr = terminalreporter
    tr.write_sep("=", "业务覆盖与用例结果", bold=True)
    by_domain: dict[str, dict[str, str]] = {}
    for reps, mark in (
        (tr.stats.get("passed", []), "PASS"),
        (tr.stats.get("failed", []), "FAIL"),
        (tr.stats.get("error", []), "ERROR"),
        (tr.stats.get("skipped", []), "SKIP"),
        (tr.stats.get("xfailed", []), "XFAIL"),
    ):
        for rep in reps:
            name = rep.nodeid.split("::", 1)[-1]
            by_domain.setdefault(_domain(rep.nodeid), {})[name] = mark
    for domain, cases in by_domain.items():
        passed = sum(1 for m in cases.values() if m == "PASS")
        tr.write_line(f"\n■ 业务域：{domain}（用例 {len(cases)}，通过 {passed}）")
        for name, mark in cases.items():
            tr.write_line(f"    [{mark}] {name}")
