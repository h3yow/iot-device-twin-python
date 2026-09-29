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
app/upgrade.py     升级：分批下发、状态机、暂停恢复、失败熔断、优先级容量约束
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
- **单设备单任务**：同一设备存在进行中任务时，任何批次都不能再向其下发；
- **同批次不重发**：同一批次中一台设备至多一个任务。已 `success/failed/timeout`
  的设备不会被重新拉进该批次——暂停恢复不会重发，熔断后即使人工恢复也不会
  再给失败/超时设备下发。设备修复后的重试请新建批次；
- **暂停/恢复**：`pause_batch` 手动暂停后停止向新设备推进（enroll/dispatch 均
  拒绝），已在跑的任务允许自然结束；`resume_batch` 后只接着尚未下发的名额推进，
  已完成/失败/超时的设备不重入，同一设备不会因暂停恢复被下发两次；
- **失败熔断**：`(failed + timeout) / 总任务数 > max_failure_ratio` 时批次自动
  暂停，`pause_reason=circuit`（手动暂停为 `manual`），可在批次查询中明确区分；
  熔断后同样可用 `resume_batch` 人工恢复，但失败/超时设备仍不会重发；
- **优先级与容量**：一台设备在任一时刻只属于一个推进中的批次。名额可提前用
  `enroll` 占用：高优先级批次（`priority` 数值更大）可接管低优先级批次**尚未
  下发**的名额，同优先级先到先得，低优先级不能抢高优先级；已下发（进行中）
  的名额是硬占用，任何优先级都不能抢。被挡批次不会产生任何中间状态；
- **名额释放**：批次暂停、熔断或完成后，未使用的名额立即释放给其他批次；
  暂停期间被别的批次接管的名额，恢复时不会抢回，批次只推进仍在自己手里的设备；
- **显式收尾**：全部任务到达终态后可 `close_batch` 置为 `completed`。

HTTP 接口：

- `POST /upgrades/batches`（body 可选 `priority`，默认 0）
- `GET  /upgrades/batches/{id}`（查看 status 与 pause_reason）
- `POST /upgrades/batches/{id}/enroll`（提前占用设备名额）
- `POST /upgrades/batches/{id}/pause`、`POST /upgrades/batches/{id}/resume`
- `POST /upgrades/batches/{id}/dispatch`（未 enroll 会原子地尝试占用名额）
- `POST /upgrades/tasks/{id}/progress`、`POST /upgrades/check-timeouts`

## 本地验证

```bash
uv sync                                    # 安装依赖
uv run python -m pytest tests/ -q -s       # 运行测试，-s 查看判定依据日志
IOT_DB_PATH=iot.db uv run uvicorn main:app # 启动服务（默认 http://127.0.0.1:8000）
```

测试覆盖：重复/乱序/迟到遥测、非法负载四类拒绝原因、100 线程并发孪生写入、
desired 乐观锁冲突、重启后状态与在线判定重建、10 线程并发重复下发、
版本门限、失败熔断、升级中断超时与新批次重下发、HTTP 端到端链路，以及
`tests/test_batch_control.py` 中的：手动暂停/恢复不重入不重发、重复暂停恢复
守卫、熔断原因标记（失败与超时两种触发）、恢复后失败设备仍不重发、
同优先级先到先得、高优先级接管未使用名额、进行中名额不可抢占、
暂停/熔断释放名额、20 线程并发 enroll 唯一占用。

### 手工复现批次控制场景

```bash
IOT_DB_PATH=demo.db uv run uvicorn main:app &

# 暂停 / 恢复：d1 在跑时暂停，暂停挡新设备，恢复后 d2 继续，d1 不会被重发
curl -s -XPOST localhost:8000/upgrades/batches -d '{
  "batch_id":"b1","to_version":"2.0.0","min_from_version":"1.0.0",
  "max_failure_ratio":0.5}'
curl -s -XPOST localhost:8000/upgrades/batches/b1/dispatch \
  -d '{"device_id":"d1","from_version":"1.2.0"}'
curl -s -XPOST localhost:8000/upgrades/batches/b1/pause
curl -s localhost:8000/upgrades/batches/b1   # status=paused, pause_reason=manual
curl -s -XPOST localhost:8000/upgrades/batches/b1/resume

# 失败熔断：门限 0.0，单任务失败即 circuit
curl -s -XPOST localhost:8000/upgrades/batches -d '{
  "batch_id":"bc","to_version":"2.0.0","min_from_version":"1.0.0",
  "max_failure_ratio":0.0}'
curl -s -XPOST localhost:8000/upgrades/batches/bc/dispatch \
  -d '{"device_id":"d9","from_version":"1.2.0"}'
# 用返回的 task_id 回报 failed，再查批次可见 status=paused/pause_reason=circuit

# 优先级争抢：低优先级先 enroll 未下发，高优先级接管，低优先级再 dispatch 被拒
curl -s -XPOST localhost:8000/upgrades/batches -d '{
  "batch_id":"low","to_version":"2.0.0","min_from_version":"1.0.0",
  "max_failure_ratio":1.0,"priority":1}'
curl -s -XPOST localhost:8000/upgrades/batches -d '{
  "batch_id":"high","to_version":"2.0.0","min_from_version":"1.0.0",
  "max_failure_ratio":1.0,"priority":10}'
curl -s -XPOST localhost:8000/upgrades/batches/low/enroll  -d '{"device_id":"dx"}'
curl -s -XPOST localhost:8000/upgrades/batches/high/enroll -d '{"device_id":"dx"}'
curl -s -XPOST localhost:8000/upgrades/batches/low/dispatch \
  -d '{"device_id":"dx","from_version":"1.2.0"}'   # 409 occupied
```

注：升级批次表对旧库自动迁移（新增 `priority` / `pause_reason` 列与
`batch_devices` 名额表），无需手工删库。
