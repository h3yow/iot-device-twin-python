# IoT 设备接入与孪生管理

基于 FastAPI + SQLite 的物联网设备接入服务，用本地持久化模拟设备上行与升级下发，
保证乱序、重复与并发场景下设备状态收敛结果可预期。

## 架构

```
main.py            HTTP 入口（FastAPI 路由）
app/models.py      领域模型与枚举（结果、拒绝原因、状态机）
app/storage.py     SQLite 持久化层（遥测日志 / 孪生 / 升级任务）
app/telemetry.py   遥测接入：校验、去重、乱序收敛
app/twin.py        孪生：并发安全更新、重启恢复、在线判定
app/upgrade.py     升级：分批下发、状态机、失败熔断
tests/             单测（日志打印关键输入与判定依据）
```

## 遥测接入语义

`POST /telemetry`，负载为 JSON 对象：

```json
{"device_id": "d1", "seq": 3, "schema_version": 1, "payload": {"temp": 22}}
```

接入结果（`result`）四选一：

| 结果 | 含义 |
|---|---|
| `applied` | seq 严格大于该设备已应用序号，已推进孪生 |
| `duplicate` | `(device_id, seq)` 已存在，幂等忽略 |
| `stale` | seq 小于已见最大序号（乱序/超时迟到），记录日志但不更新状态 |
| `rejected` | 负载非法，HTTP 422，附可区分 `reason` |

拒绝原因（`reason`）：`invalid_json`（非 JSON/非对象）、`missing_field`（缺字段）、
`invalid_type`（字段类型不符）、`invalid_version`（schema_version 不支持）。

收敛保证：遥测日志按 `(device_id, seq)` 主键幂等落盘；孪生 reported 只接受
`seq > reported_version` 的数据，因此重复、乱序、迟到均不会破坏最新状态。

## 孪生收敛规则

- **reported**：单调推进。仅当遥测 seq 严格大于 `reported_version` 时合入，
  陈旧数据直接丢弃，绝不回退。
- **desired**：乐观并发控制。`PATCH /twins/{id}/desired` 携带 `expected_version`，
  与当前 `desired_version` 不符即返回 409 `version_conflict`，杜绝陈旧覆盖；
  成功则版本单调 +1。
- **并发**：每设备一把锁，同一设备的并发写入串行化。
- **重启恢复**：每次变更同步落盘 SQLite；启动时 `recover()` 重建全部孪生。
- **在线判定**：不持久化，由 `now - last_seen < 60s` 动态计算，
  重启后按持久化的 `last_seen` 一致重建。

## 升级状态机

单设备任务：`pending -> downloading -> flashing -> success`，
任何非终态可转 `failed` / `cancelled`；超时由 `check_timeouts` 置为 `timeout`。
终态（success/failed/timeout/cancelled）不可再迁移。

批次约束：

- **版本门限**：设备 `from_version >= min_from_version` 才允许进入批次；
- **单设备单任务**：同一设备存在进行中任务时，重复下发返回 409；
- **失败熔断**：`(failed + timeout) / 总任务数 > max_failure_ratio` 时批次自动
  `paused`，停止继续下发；
- **显式收尾**：全部任务到达终态后可 `close_batch` 置为 `completed`。

HTTP 接口：`POST /upgrades/batches`、`POST /upgrades/batches/{id}/dispatch`、
`POST /upgrades/tasks/{id}/progress`、`POST /upgrades/check-timeouts`。

## 本地验证

```bash
uv sync                                    # 安装依赖
uv run python -m pytest tests/ -q -s       # 运行测试，-s 查看判定依据日志
IOT_DB_PATH=iot.db uv run uvicorn main:app # 启动服务（默认 http://127.0.0.1:8000）
```

测试覆盖：重复/乱序/迟到遥测、非法负载四类拒绝原因、100 线程并发孪生写入、
desired 乐观锁冲突、重启后状态与在线判定重建、10 线程并发重复下发、
版本门限、失败熔断、升级中断超时与重下发、HTTP 端到端链路。
