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
app/upgrade.py     升级：分批下发、状态机、暂停/恢复、失败熔断、优先级与设备容量
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
- **单设备单属主**：同一设备任一时刻只有一个属主批次、一个进行中任务；
  更高优先级（或同优先级更早创建）的 RUNNING 批次可以抢占，原在途任务置
  `cancelled`；低优批次的争抢返回 409（详见下文"批次优先级与设备占用"）；
- **批次内设备一次**：同一批次中同一台设备至多被下发一次。已经 `success` /
  `failed` / `timeout` / `cancelled` 的设备，在该批次内无论后续怎么重试推进
  （暂停/恢复、熔断/恢复）都不会再收到新任务；设备修好后的重试请开新批次；
- **失败熔断**：`(failed + timeout) / 总任务数 > max_failure_ratio` 时批次自动
  暂停，`status=paused` 且 `pause_reason=circuit_breaker`，与正常执行完
  （`completed`）和手动暂停（`pause_reason=manual`）明确区分；
- **暂停 / 恢复**：`POST .../{id}/pause` 手动暂停后批次不再向任何设备下发，
  已在跑（pending/downloading/flashing）的任务允许自然结束；
  `POST .../{id}/resume` 后按原进度继续——已终态设备不会被重新拉起，
  暂停/恢复本身也不会导致同一设备被下发两次。熔断暂停同样可以手动恢复，
  恢复后仅向尚未下发过的设备推进；
- **显式收尾**：全部任务到达终态后可 `close_batch` 置为 `completed`。

## 批次优先级与设备占用

`create_batch` 接受可选 `priority`（整数，默认 0，数值越大优先级越高）。

### 属主与排名

同一台设备在任一时刻**只归属于一个正在推进（RUNNING）的属主批次**。属主从
"RUNNING 且名下已有该设备任务"的批次，加上当前下发请求方，一起按
`priority` 降序、创建时间（`created_at`）升序、`batch_id` 升序排名，
排名最高者为属主。因此：

- 多个批次争抢同一批设备时，高优先级批次一定先占住设备；同优先级时先创建的
  批次优先，结果与下发到达先后无关；
- 低优先级批次不能把设备从属主手里抢走：下发直接返回 409 `cannot preempt`，
  **不创建任何任务行**，没有半推半就的中间状态，批次仍可下发其他设备。

### 抢占（硬占用）

高优先级（或同优先级更早创建）批次后到、而设备上还跑着低优批次的在途任务时，
高优请求**不必等待**原任务跑完或超时：原在途任务被干净地置为 `cancelled`
（终态，不计入失败率、不触发熔断），新任务立即创建并占用设备。

- 被抢占批次名下保留这条 `cancelled` 终态记录。受"同一批次内设备只下发一次"
  约束，该批次之后无论继续推进还是暂停后恢复，都不能再向这台设备下发，
  因此设备不可能被低优批次抢回，也不可能被同一批次下发两次；
- 被抢占任务后续再回报进度会被终态规则拒绝（`already terminal`），
  陈旧回报不会污染接管方任务的状态；
- 下发接口响应带 `preempted_task_id` / `preempted_batch_id`，明确告知本次
  是否发生了抢占以及取消的是哪个任务。

### 名额释放与暂停/恢复的兼容范围

- 批次**手动暂停或失败熔断的瞬间即退出属主排名**，它原先占住的设备名额马上
  可以被其他 RUNNING 批次接管；正常 `completed` 收尾同样释放；
- 兼容范围：暂停/熔断本身**不主动取消**在途任务——没有其他批次争抢时，已经
  在跑（pending/downloading/flashing）的任务仍允许自然结束；一旦其他 RUNNING
  批次来下发这台设备，在途任务立即按上面的抢占规则取消并交接，不会一直挂到
  超时；
- 暂停（`pause_reason=manual`）与熔断（`pause_reason=circuit_breaker`）的批次
  都可以 `resume` 恢复；恢复后按同一套排名规则重新参与，已被其他批次占住的
  设备不会被自动抢回，仅向尚未下发过的设备推进。

### 状态可查询

不必靠下发报错反推批次状态：

- `GET /upgrades/batches/{id}`：返回 `status`（`running` / `paused` /
  `completed`）与 `pause_reason`（`manual` / `circuit_breaker` / 完成时为
  `null`），直接区分手动暂停、熔断停住、正常跑完；
- `GET /upgrades/batches`：按名额争抢排名顺序列出全部批次。

HTTP 接口：`POST /upgrades/batches`（可带 `priority`）、
`GET /upgrades/batches`、`GET /upgrades/batches/{id}`、
`POST /upgrades/batches/{id}/pause`、`POST /upgrades/batches/{id}/resume`、
`POST /upgrades/batches/{id}/dispatch`、
`POST /upgrades/tasks/{id}/progress`、`POST /upgrades/check-timeouts`。

## 本地验证

```bash
uv sync                                              # 安装依赖
uv run python -m pytest tests/ -v                    # 运行测试；结尾按业务域
                                                     # 分组列出用例与 PASS/FAIL
uv run python -m pytest tests/ -v -s                 # 额外查看各用例判定依据日志
IOT_DB_PATH=iot.db uv run uvicorn main:app           # 启动服务（默认 http://127.0.0.1:8000）
```

批次管理场景的 curl 复现（旧版本本地数据库会自动迁移新增列）：

```bash
# 建批次（max_failure_ratio=0.5，可选 priority）
curl -s -XPOST localhost:8000/upgrades/batches -d '{
  "batch_id":"b1","to_version":"2.0.0","min_from_version":"1.0.0",
  "max_failure_ratio":0.5,"priority":1}'

# 下发两台并回报：一台成功、一台失败（失败率 1/2 未超 0.5，再失败即熔断）
curl -s -XPOST localhost:8000/upgrades/batches/b1/dispatch \
  -d '{"device_id":"d1","from_version":"1.2.0"}'
curl -s -XPOST localhost:8000/upgrades/batches/b1/dispatch \
  -d '{"device_id":"d2","from_version":"1.2.0"}'
# 暂停后再下发：409 dispatch stopped；在途任务仍可 POST .../progress 自然结束
curl -s -XPOST localhost:8000/upgrades/batches/b1/pause
curl -s -XPOST localhost:8000/upgrades/batches/b1/resume

# 跨批次争抢同一台设备：低优先下发（在途），高优后到
# 1) 低优批次（priority=1）先把 d1 下发出去并推进到 downloading
curl -s -XPOST localhost:8000/upgrades/batches -d '{
  "batch_id":"b-low","to_version":"2.0.0","min_from_version":"1.0.0",
  "max_failure_ratio":1.0,"priority":1}'
curl -s -XPOST localhost:8000/upgrades/batches/b-low/dispatch \
  -d '{"device_id":"d1","from_version":"1.2.0"}'   # -> task T-low
curl -s -XPOST localhost:8000/upgrades/tasks/T-low/progress \
  -d '{"status":"downloading"}'

# 2) 中优批次（priority=5）来抢：409 cannot preempt，任务表无 b-mid/d1 记录
curl -s -XPOST localhost:8000/upgrades/batches -d '{
  "batch_id":"b-mid","to_version":"2.5.0","min_from_version":"1.0.0",
  "max_failure_ratio":1.0,"priority":5}'
curl -s -XPOST localhost:8000/upgrades/batches/b-mid/dispatch \
  -d '{"device_id":"d1","from_version":"1.2.0"}'   # -> 409

# 3) 高优批次（priority=10）后到：立即抢占，响应带 preempted_task_id=T-low；
#    T-low 变为 cancelled（不计失败），d1 唯一在途任务属于 b-high
curl -s -XPOST localhost:8000/upgrades/batches -d '{
  "batch_id":"b-high","to_version":"3.0.0","min_from_version":"1.0.0",
  "max_failure_ratio":1.0,"priority":10}'
curl -s -XPOST localhost:8000/upgrades/batches/b-high/dispatch \
  -d '{"device_id":"d1","from_version":"1.2.0"}'

# 4) 低优暂停恢复后仍抢不回：409 already dispatched
curl -s -XPOST localhost:8000/upgrades/batches/b-low/pause
curl -s -XPOST localhost:8000/upgrades/batches/b-low/resume
curl -s -XPOST localhost:8000/upgrades/batches/b-low/dispatch \
  -d '{"device_id":"d1","from_version":"1.2.0"}'   # -> 409

# 5) 直接查询批次状态与停下原因（不用靠下发报错猜）
curl -s localhost:8000/upgrades/batches/b-low   # status=paused 或 running
curl -s localhost:8000/upgrades/batches         # 按优先级排名列出

# 6) 名额释放：批次 pause / 熔断后立即退出属主排名；其他 RUNNING 批次来
#    下发其在途设备时立即接管（原任务 cancelled），无需等超时
```

> 直接用 Python 复现并发与随机调度顺序的场景见
> `tests/test_upgrade.py::test_concurrent_cross_batch_dispatch_single_winner_no_half_state`
> （12 线程跨 3 个优先级批次抢同一台设备）。

测试覆盖（共 4 个业务域，23 个用例，运行后结尾按业务域分组列出每条用例及
PASS/FAIL 结果）：

- **遥测接入**：重复幂等、乱序/迟到不回退、四类拒绝原因可区分；
- **设备孪生**：并发 reported 收敛到最大 seq、desired 乐观锁防陈旧覆盖、
  重启后状态与在线判定重建；
- **升级批次**：10 线程同批次并发重复下发、版本门限、失败熔断可区分、
  终态拒绝迁移、超时判定、暂停停发/在途自然结束/恢复续跑不重发、
  失败/超时设备永不重发、终态名额归属链、熔断释放名额、同优先级先创建者优先；
- **优先级与名额（本次修复）**：低优在途先下发、高优后到立即抢占、
  被抢占任务 cancelled 且不计失败、被抢占方继续推进与暂停恢复都抢不回、
  被挡下批次零残留、12 线程跨 3 优先级批次并发争抢（唯一在途任务必属最高优、
  其余仅 cancelled 终态行）、手动暂停与熔断后在途名额被争抢时立即释放；
- **HTTP 端到端**：遥测-孪生-升级全链路、抢占响应字段、批次状态查询与
  manual/circuit_breaker 区分。
