"""通用小工具。"""
from __future__ import annotations

import time


def now_ms() -> int:
    """当前时间，毫秒。"""
    return int(time.time() * 1000)


def parse_version(text: str) -> tuple[int, ...]:
    """把 '1.2.3' 形式的版本号解析为可比较的元组；非法输入抛 ValueError。"""
    parts = text.strip().split(".")
    if not parts or any(not p.isdigit() for p in parts):
        raise ValueError(f"invalid version string: {text!r}")
    return tuple(int(p) for p in parts)
