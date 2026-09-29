"""本地 SQLite 持久化层：遥测日志、孪生状态、升级任务。"""
from __future__ import annotations

import json
import sqlite3
import threading

from app.models import (
    BatchStatus,
    BatchDevice,
    DeviceOccupancy,
    DeviceSlotStatus,
    PauseReason,
    TelemetryRecord,
    TwinState,
    UpgradeBatch,
    UpgradeStatus,
    UpgradeTask,
    TERMINAL_UPGRADE_STATES,
)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS telemetry (
    device_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    schema_version INTEGER NOT NULL,
    payload TEXT NOT NULL,
    received_at REAL NOT NULL,
    PRIMARY KEY (device_id, seq)
);
CREATE TABLE IF NOT EXISTS twins (
    device_id TEXT PRIMARY KEY,
    reported TEXT NOT NULL,
    desired TEXT NOT NULL,
    reported_version INTEGER NOT NULL,
    desired_version INTEGER NOT NULL,
    last_seen REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS upgrade_batches (
    batch_id TEXT PRIMARY KEY,
    to_version TEXT NOT NULL,
    min_from_version TEXT NOT NULL,
    max_failure_ratio REAL NOT NULL,
    status TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0,
    pause_reason TEXT
);
CREATE TABLE IF NOT EXISTS upgrade_tasks (
    task_id TEXT PRIMARY KEY,
    device_id TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    from_version TEXT NOT NULL,
    to_version TEXT NOT NULL,
    status TEXT NOT NULL,
    updated_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_tasks_device ON upgrade_tasks(device_id, status);
CREATE TABLE IF NOT EXISTS batch_devices (
    batch_id TEXT NOT NULL,
    device_id TEXT NOT NULL,
    slot TEXT NOT NULL,
    PRIMARY KEY (batch_id, device_id)
);
CREATE INDEX IF NOT EXISTS idx_batch_devices_device ON batch_devices(device_id, slot);
"""

_TERMINAL = tuple(s.value for s in TERMINAL_UPGRADE_STATES)


class Storage:
    """线程安全的 SQLite 存储，服务重启后据此重建状态。"""

    def __init__(self, db_path: str = ":memory:") -> None:
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock, self._conn:
            self._conn.executescript(_SCHEMA)
            # 兼容旧库：upgrade_batches 早期版本没有 priority / pause_reason 列
            cols = {
                r["name"]
                for r in self._conn.execute("PRAGMA table_info(upgrade_batches)").fetchall()
            }
            if "priority" not in cols:
                self._conn.execute(
                    "ALTER TABLE upgrade_batches ADD COLUMN priority INTEGER NOT NULL DEFAULT 0"
                )
            if "pause_reason" not in cols:
                self._conn.execute(
                    "ALTER TABLE upgrade_batches ADD COLUMN pause_reason TEXT"
                )

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ---- 遥测 ----
    def insert_telemetry(self, record: TelemetryRecord) -> bool:
        """幂等插入；重复 (device_id, seq) 返回 False。"""
        with self._lock, self._conn:
            cur = self._conn.execute(
                "INSERT OR IGNORE INTO telemetry VALUES (?,?,?,?,?)",
                (
                    record.device_id,
                    record.seq,
                    record.schema_version,
                    json.dumps(record.payload, sort_keys=True),
                    record.received_at,
                ),
            )
            return cur.rowcount == 1

    def max_seq(self, device_id: str) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT COALESCE(MAX(seq), 0) AS m FROM telemetry WHERE device_id=?",
                (device_id,),
            ).fetchone()
            return int(row["m"])

    def telemetry_after(self, device_id: str, seq: int) -> list[TelemetryRecord]:
        """恢复用：取出 seq 大于给定值的全部遥测，按 seq 升序。"""
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM telemetry WHERE device_id=? AND seq>? ORDER BY seq",
                (device_id, seq),
            ).fetchall()
        return [
            TelemetryRecord(
                device_id=r["device_id"],
                seq=r["seq"],
                schema_version=r["schema_version"],
                payload=json.loads(r["payload"]),
                received_at=r["received_at"],
            )
            for r in rows
        ]

    def all_device_ids(self) -> list[str]:
        with self._lock:
            rows = self._conn.execute("SELECT DISTINCT device_id FROM telemetry").fetchall()
            return [r["device_id"] for r in rows]

    # ---- 孪生 ----
    def load_twins(self) -> dict[str, TwinState]:
        with self._lock:
            rows = self._conn.execute("SELECT * FROM twins").fetchall()
        return {
            r["device_id"]: TwinState(
                device_id=r["device_id"],
                reported=json.loads(r["reported"]),
                desired=json.loads(r["desired"]),
                reported_version=r["reported_version"],
                desired_version=r["desired_version"],
                last_seen=r["last_seen"],
            )
            for r in rows
        }

    def save_twin(self, twin: TwinState) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO twins VALUES (?,?,?,?,?,?) "
                "ON CONFLICT(device_id) DO UPDATE SET "
                "reported=excluded.reported, desired=excluded.desired, "
                "reported_version=excluded.reported_version, "
                "desired_version=excluded.desired_version, "
                "last_seen=excluded.last_seen",
                (
                    twin.device_id,
                    json.dumps(twin.reported, sort_keys=True),
                    json.dumps(twin.desired, sort_keys=True),
                    twin.reported_version,
                    twin.desired_version,
                    twin.last_seen,
                ),
            )

    # ---- 升级 ----
    def save_batch(self, batch: UpgradeBatch) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO upgrade_batches VALUES (?,?,?,?,?,?,?) "
                "ON CONFLICT(batch_id) DO UPDATE SET "
                "status=excluded.status, priority=excluded.priority, "
                "pause_reason=excluded.pause_reason",
                (
                    batch.batch_id,
                    batch.to_version,
                    batch.min_from_version,
                    batch.max_failure_ratio,
                    batch.status.value,
                    batch.priority,
                    batch.pause_reason.value if batch.pause_reason else None,
                ),
            )

    def load_batch(self, batch_id: str) -> UpgradeBatch | None:
        with self._lock:
            r = self._conn.execute(
                "SELECT * FROM upgrade_batches WHERE batch_id=?", (batch_id,)
            ).fetchone()
        if r is None:
            return None
        reason = r["pause_reason"]
        return UpgradeBatch(
            batch_id=r["batch_id"],
            to_version=r["to_version"],
            min_from_version=r["min_from_version"],
            max_failure_ratio=r["max_failure_ratio"],
            status=BatchStatus(r["status"]),
            priority=r["priority"],
            pause_reason=PauseReason(reason) if reason else None,
        )

    def save_task(self, task: UpgradeTask) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO upgrade_tasks VALUES (?,?,?,?,?,?,?) "
                "ON CONFLICT(task_id) DO UPDATE SET "
                "status=excluded.status, updated_at=excluded.updated_at",
                (
                    task.task_id,
                    task.device_id,
                    task.batch_id,
                    task.from_version,
                    task.to_version,
                    task.status.value,
                    task.updated_at,
                ),
            )

    def load_tasks(self, batch_id: str | None = None) -> list[UpgradeTask]:
        sql = "SELECT * FROM upgrade_tasks"
        args: tuple = ()
        if batch_id is not None:
            sql += " WHERE batch_id=?"
            args = (batch_id,)
        with self._lock:
            rows = self._conn.execute(sql, args).fetchall()
        return [
            UpgradeTask(
                task_id=r["task_id"],
                device_id=r["device_id"],
                batch_id=r["batch_id"],
                from_version=r["from_version"],
                to_version=r["to_version"],
                status=UpgradeStatus(r["status"]),
                updated_at=r["updated_at"],
            )
            for r in rows
        ]

    def active_task_for_device(self, device_id: str) -> UpgradeTask | None:
        placeholders = ",".join("?" for _ in _TERMINAL)
        with self._lock:
            r = self._conn.execute(
                f"SELECT * FROM upgrade_tasks WHERE device_id=? "
                f"AND status NOT IN ({placeholders}) ORDER BY updated_at DESC LIMIT 1",
                (device_id, *_TERMINAL),
            ).fetchone()
        if r is None:
            return None
        return UpgradeTask(
            task_id=r["task_id"],
            device_id=r["device_id"],
            batch_id=r["batch_id"],
            from_version=r["from_version"],
            to_version=r["to_version"],
            status=UpgradeStatus(r["status"]),
            updated_at=r["updated_at"],
        )

    # ---- 批次设备名额 ----
    def save_batch_device(self, entry: BatchDevice) -> None:
        """幂等写入/更新 (batch, device) 的名额状态。"""
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO batch_devices VALUES (?,?,?) "
                "ON CONFLICT(batch_id, device_id) DO UPDATE SET slot=excluded.slot",
                (entry.batch_id, entry.device_id, entry.slot.value),
            )

    def load_batch_devices(self, batch_id: str) -> list[BatchDevice]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM batch_devices WHERE batch_id=?", (batch_id,)
            ).fetchall()
        return [
            BatchDevice(
                batch_id=r["batch_id"],
                device_id=r["device_id"],
                slot=DeviceSlotStatus(r["slot"]),
            )
            for r in rows
        ]

    def reserved_slot(self, batch_id: str, device_id: str) -> BatchDevice | None:
        """该批次是否持有该设备的有效名额。"""
        with self._lock:
            r = self._conn.execute(
                "SELECT * FROM batch_devices WHERE batch_id=? AND device_id=? AND slot=?",
                (batch_id, device_id, DeviceSlotStatus.RESERVED.value),
            ).fetchone()
        if r is None:
            return None
        return BatchDevice(
            batch_id=r["batch_id"],
            device_id=r["device_id"],
            slot=DeviceSlotStatus(r["slot"]),
        )

    def device_occupancy(self, device_id: str) -> DeviceOccupancy | None:
        """设备当前被哪个批次占用：有效名额的批次且批次仍在 RUNNING 才算占住名额。

        暂停/熔断/完成的批次名额视为已释放；同设备理论上至多一个有效占用。
        注意：名额是否“已使用（已下发过任务）”由 service 层进一步判定。
        """
        with self._lock:
            r = self._conn.execute(
                "SELECT bd.batch_id AS batch_id, bd.device_id AS device_id "
                "FROM batch_devices bd "
                "JOIN upgrade_batches ub ON ub.batch_id = bd.batch_id "
                "WHERE bd.device_id=? AND bd.slot=? AND ub.status=? "
                "ORDER BY ub.priority DESC LIMIT 1",
                (
                    device_id,
                    DeviceSlotStatus.RESERVED.value,
                    BatchStatus.RUNNING.value,
                ),
            ).fetchone()
        if r is None:
            return None
        return DeviceOccupancy(device_id=r["device_id"], batch_id=r["batch_id"])

    def reserved_batches_for_device(self, device_id: str) -> list[str]:
        """所有对该设备持有 RESERVED 名额的批次 id，按批次优先级降序。"""
        with self._lock:
            rows = self._conn.execute(
                "SELECT bd.batch_id AS batch_id FROM batch_devices bd "
                "JOIN upgrade_batches ub ON ub.batch_id = bd.batch_id "
                "WHERE bd.device_id=? AND bd.slot=? "
                "ORDER BY ub.priority DESC",
                (device_id, DeviceSlotStatus.RESERVED.value),
            ).fetchall()
        return [r["batch_id"] for r in rows]
