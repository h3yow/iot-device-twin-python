"""孪生：并发更新无陈旧覆盖、重启后状态与在线判定一致重建。"""
import threading

import pytest

from app.storage import Storage
from app.twin import TwinService, ConflictError

DB = "test_twin.db"


@pytest.fixture()
def db_path(tmp_path):
    return str(tmp_path / "twin.db")


def test_concurrent_reported_updates_converge_to_max_seq(db_path):
    storage = Storage(db_path)
    svc = TwinService(storage)
    svc.recover()
    n = 100
    barrier = threading.Barrier(n)

    def worker(seq):
        barrier.wait()  # 同时放行，最大化交错
        svc.apply_reported("d1", seq, {f"k{seq}": seq}, received_at=1000.0 + seq)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(1, n + 1)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    twin = svc.get("d1")
    print(f"[判定依据] {n} 个并发写入 seq=1..{n}，收敛 reported_version="
          f"{twin.reported_version}（应等于最大 seq={n}），"
          f"最高序号键 k{n}={twin.reported.get(f'k{n}')}")
    # 收敛保证：版本推进到最大 seq，且最新快照已生效；
    # 乱序到达的低 seq 被丢弃属预期（陈旧不得覆盖最新）。
    assert twin.reported_version == n
    assert twin.reported[f"k{n}"] == n
    storage.close()


def test_concurrent_desired_updates_no_stale_overwrite(db_path):
    storage = Storage(db_path)
    svc = TwinService(storage)
    svc.recover()
    svc.update_desired("d1", {"init": True}, expected_version=0)
    conflicts = []
    barrier = threading.Barrier(2)

    def worker(name):
        barrier.wait()
        try:
            svc.update_desired("d1", {name: 1}, expected_version=1)  # 双方都基于 v1
        except ConflictError as e:
            conflicts.append(str(e))

    t1 = threading.Thread(target=worker, args=("a",))
    t2 = threading.Thread(target=worker, args=("b",))
    t1.start(); t2.start(); t1.join(); t2.join()

    twin = svc.get("d1")
    print(f"[判定依据] 两线程同以 expected_version=1 提交，冲突数={len(conflicts)}，"
          f"最终 desired_version={twin.desired_version} desired={twin.desired}")
    assert len(conflicts) == 1            # 恰有一个被拒，杜绝陈旧覆盖
    assert twin.desired_version == 2
    assert ("a" in twin.desired) != ("b" in twin.desired)
    storage.close()


def test_restart_recovers_state_and_online_judgement(db_path):
    now = [10_000.0]
    storage = Storage(db_path)
    svc = TwinService(storage, clock=lambda: now[0], online_ttl=60.0)
    svc.recover()
    svc.apply_reported("d1", 5, {"temp": 25}, received_at=now[0] - 10)   # 10s 前在线
    svc.apply_reported("d2", 3, {"temp": 18}, received_at=now[0] - 3600) # 1h 前离线
    svc.update_desired("d1", {"target": 26})
    before = (svc.get("d1").online, svc.get("d2").online)
    storage.close()

    # 模拟重启：新 Storage + 新 TwinService，同一时钟
    storage2 = Storage(db_path)
    svc2 = TwinService(storage2, clock=lambda: now[0], online_ttl=60.0)
    svc2.recover()
    d1, d2 = svc2.get("d1"), svc2.get("d2")
    print(f"[判定依据] 重启前 online(d1,d2)={before}，"
          f"重启后 d1: reported={d1.reported} v={d1.reported_version} "
          f"desired_v={d1.desired_version} online={d1.online}; d2 online={d2.online}")
    assert before == (True, False)
    assert d1.reported == {"temp": 25} and d1.reported_version == 5
    assert d1.desired == {"target": 26} and d1.desired_version == 1
    assert (d1.online, d2.online) == (True, False)  # 在线判定一致重建
    storage2.close()
