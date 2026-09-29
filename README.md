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
- **单设备单任务**：同一设备存在进行中任务时，重复下发返回 409；
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

## 批次优先级与设备容量

`create_batch` 接受可选 `priority`（整数，默认 0，数值越大优先级越高）。

- 同一台设备在任一时刻只归属于一个**正在推进（RUNNING）的属主批次**：属主是
  "RUNNING 且名下已有该设备任务"的批次中排名最高者，排名按
  `priority` 降序、创建时间升序、`batch_id` 升序决出。
- 下发时当前批次必须就是属主：高优先级批次可以先占用设备（从仍 RUNNING 的
  低优批次手里"抢"到尚未被下发的终态名额），被挡下的低优批次**不能**把设备
  从别人手里抢走；所有判定与落盘在同一把锁内完成，被挡下时不会创建任务，
  不留下半推半就的中间状态（HTTP 409，任务表中无该批次该设备的记录）。
- 批次被**暂停（手动或熔断）或收尾后不再参与属主排名**，它占用的设备名额
  立即顺延释放给其他 RUNNING 批次；批次恢复后按上面的排名规则重新参与，
  不会自动抢回已被其他批次占用的设备。
- 兼容范围：设备名额是"软占用"——高优批次仍可向尚无更高属主的设备下发；
  优先级只决定争抢结果，不会取消或迁移其他批次已经在跑的任务；进行中的
  设备仍受全局"单设备单进行中任务"约束，其他批次须等其自然结束。

HTTP 接口：`POST /upgrades/batches`（可带 `priority`）、
`POST /upgrades/batches/{id}/pause`、`POST /upgrades/batches/{id}/resume`、
`POST /upgrades/batches/{id}/dispatch`、
`POST /upgrades/tasks/{id}/progress`、`POST /upgrades/check-timeouts`。

## 本地验证

```bash
uv sync                                    # 安装依赖
uv run python -m pytest tests/ -q -s       # 运行测试，-s 查看判定依据日志
IOT_DB_PATH=iot.db uv run uvicorn main:app # 启动服务（默认 http://127.0.0.1:8000）
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

# 跨批次争抢：b2 priority=10 可先占设备；低优批次同设备下发返回 409 cannot preempt；
# 高优批次 pause/熔断后，名额释放给其他批次
```

测试覆盖：重复/乱序/迟到遥测、非法负载四类拒绝原因、100 线程并发孪生写入、
desired 乐观锁冲突、重启后状态与在线判定重建、10 线程并发重复下发、
版本门限、失败熔断（`pause_reason=circuit_breaker` 可区分）、
升级中断超时与跨批次重试、暂停后停发/在途任务自然结束/恢复续跑不重发、
熔断原因识别与失败/超时设备永不重发、跨批次优先级争抢（含低优被挡无残留、
暂停与熔断释放名额、同优先级先创建者优先）、12 线程跨批次并发争抢无中间状态、
HTTP 端到端链路。
