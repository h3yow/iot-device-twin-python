# IoT 设备接入与数字孪生服务（本地存储版）

基于 FastAPI 的物联网设备接入服务，用本地 JSON 文件模拟持久化，重点保证
**乱序 / 重复 / 并发** 场景下设备状态的收敛结果可预期、可复现。

## 1. 能力总览

| 能力 | 入口 | 说明 |
| --- | --- | --- |
| 设备注册 | `POST /devices/{device_id}` | 未注册设备的遥测一律拒绝 |
| 遥测上行 | `POST /devices/{device_id}/telemetry` | 校验、去重、按序收敛，全部事件落本地日志 |
| 孪生查询 | `GET /devices/{device_id}/twin` | 返回最新 reported、版本号与在线判定 |
| 孪生更新 | `PATCH /devices/{device_id}/twin` | 乐观并发控制（OCC），杜绝陈旧覆盖 |
| 创建批次 | `POST /ota/batches` | 版本门限 + 批次并发上限 |
| 批次推进 | `POST /ota/batches/{batch_id}/dispatch` | 返回 started 与每个被跳过设备的原因 |
| 结果上报 | `POST /ota/tasks/{task_id}/status` | 严格状态机校验 |
| 超时扫描 | `POST /ota/sweep` | 超时任务转 TIMED_OUT 并被永久隔离 |

## 2. 遥测接入语义

请求体：

```json
{ "ts": 1726000000000, "seq": 42, "data": {"temp": 21.5, "fw_version": "1.2.0"} }
```

- `ts`：设备侧时间戳（毫秒，整数）；`seq`：设备单调序号（非负整数）；
  `data`：仅允许**标量字段**（数字 / 字符串 / 布尔 / null）。

### 2.1 失败必须被拒绝，且原因可区分

| 情况 | `error` | HTTP 状态码 |
| --- | --- | --- |
| 非法 JSON（含二进制乱码） | `unparseable_payload` | 400 |
| 缺 `ts` / `seq` / `data` | `missing_field` | 422 |
| 类型不兼容（`ts` 非整数、`seq` 负数、`data` 内嵌对象/数组） | `incompatible_field_type` | 422 |
| 设备未注册 | `unknown_device` | 404 |

失败事件**不会**写入持久化日志。

### 2.2 受理后的三种结果（200，`outcome` 字段区分）

以 `(ts, seq)` 作为设备水位（先比 `ts`，相同再比 `seq`）：

- `applied`：新于当前水位 → 覆盖同名 reported 字段、推进水位与版本号；
- `duplicate`：`(ts, seq)` 与历史事件完全相同 → 幂等忽略，不重复落日志；
- `late`：乱序到达或超时迟到（旧于水位）→ **写入 `telemetry.jsonl` 用于审计/重放，
  但不覆盖最新孪生状态**。

同 `ts` 时按 `seq` 判定新旧，因此同毫秒上报也有确定结果。

## 3. 孪生收敛规则

- **并发模型**：每设备一把互斥锁，遥测应用与 PATCH 串行化；
  PATCH 携带 `expected_version`，版本不匹配返回 `409 version_conflict`，
  调用方基于最新版本重试（见 `tests/test_twin.py`），8 线程并发写入后
  所有键均合并、版本恰好递增，无陈旧覆盖。
- **字段合并**：新遥测只覆盖 `data` 中出现的同名键，PATCH 写入的其它键保留，
  使遥测与业务写入收敛到同一份单调合并状态；每次 applied/PATCH 版本号 +1。
- **在线判定**：`last_seen`（服务端接收时间）距今不超过 `ONLINE_TTL_MS`
  （默认 60 秒）即为在线。
- **重启恢复**：启动时以 `twins.json` 快照打底，再按 `telemetry.jsonl`
  的接收顺序重放全部遥测（重放走同一套重复/乱序规则，不会重复计数），
  重建水位、reported 与在线判定。可复现：重启后再发重复/迟到事件，
  结论与重启前一致。

## 4. 固件升级状态机

```
PENDING ──► IN_PROGRESS ──► SUCCESS
                │         ──► FAILED
                │         ──► TIMED_OUT   (sweep 扫描判定)
                └─────────► ABORTED
SUCCESS / FAILED / TIMED_OUT / ABORTED 均为终态
```

- dispatch 创建任务即迁移到 `IN_PROGRESS`（无驻留 PENDING）。
- 终态收到任何上报（含相同状态的重复上报）→ `422 already_terminal`；
  `IN_PROGRESS` 收到非法迁移 → `422 illegal_transition`。

### 4.1 推进约束（dispatch 的 skipped 原因逐条给出）

| 跳过原因 | 判定依据 |
| --- | --- |
| `device_busy` | 该设备在**任意批次**存在非终态任务（单设备至多一个进行中升级） |
| `already_dispatched` | 本批次任务在途（重复下发幂等） |
| `already_success` / `already_failed` / `already_timed_out` / `already_aborted` | 本批次任务已终态，**永不再次推进** |
| `missing_fw_version` | 孪生中无 `fw_version`，无法判定门限 |
| `below_version_gate` | 当前版本 < 批次 `min_current_version`，**门限不可绕过** |
| `invalid_device_version` | 设备版本号格式非法 |
| `concurrency_limit` | 批次内 IN_PROGRESS 数量已达 `max_concurrent` |

设备级约束（重复 / 忙 / 门限）先于并发上限判定，因此并发槽位占满时
门限不满足的设备仍会被明确报为 `below_version_gate`，不会被槽位掩盖。

### 4.2 失败与超时的可识别、可中断

- 设备上报 `failed` 或被 `/ota/sweep` 判定 `timed_out` 后进入终态；
- 后续任何 dispatch 对该设备返回 `already_failed` / `already_timed_out`，
  该设备被永久隔离，批次槽位释放给其它设备；
- 所有批次/任务状态在每次变更时原子写入 `ota.json`，重启后原样恢复。

## 5. 本地持久化布局

数据目录（默认 `./data`，可用 `Services(data_dir=...)` 覆盖）：

```
data/
├── devices.json     # 设备注册表（原子替换写）
├── telemetry.jsonl  # 遥测事件追加日志（每条 fsync）
├── twins.json       # 孪生快照（临时文件 + os.replace 原子替换）
└── ota.json         # 升级批次与任务（原子替换写）
```

进程在快照写入中途被杀死不会留下半文件；遥测日志为 append-only，
是重启重放的事实来源。

## 6. 本地验证方法

```bash
# 环境准备
uv sync

# 运行全部单测（日志会打印每个用例的关键输入与判定依据）
uv run pytest

# 仅看某类场景
uv run pytest tests/test_telemetry.py -q   # 重复 / 乱序 / 迟到 / 拒绝原因
uv run pytest tests/test_twin.py -q       # 并发 OCC / 锁 / 重启恢复
uv run pytest tests/test_ota.py -q        # 批次 / 门限 / 中断 / 重复下发
uv run pytest tests/test_api.py -q        # HTTP 端到端 + 重启

# 启动服务
uv run fastapi dev main.py --port 8000
# 交互文档： http://127.0.0.1:8000/docs
```

### curl 速览

```bash
curl -X POST localhost:8000/devices/dev1
curl -X POST localhost:8000/devices/dev1/telemetry \
  -H 'Content-Type: application/json' \
  -d '{"ts":1726000000000,"seq":1,"data":{"temp":21.5,"fw_version":"1.0.0"}}'
curl localhost:8000/devices/dev1/twin

curl -X POST localhost:8000/ota/batches -H 'Content-Type: application/json' -d '{
  "batch_id":"B1","to_version":"2.0.0","min_current_version":"1.0.0",
  "max_concurrent":2,"device_ids":["dev1"]}'
curl -X POST localhost:8000/ota/batches/B1/dispatch
curl -X POST localhost:8000/ota/tasks/B1:dev1/status \
  -H 'Content-Type: application/json' -d '{"status":"success"}'
```

## 7. 代码结构

```
app/
├── models.py    # 枚举、状态迁移表、领域 dataclass
├── storage.py   # 本地 JSON/JSONL 原子持久化
├── twin.py      # 每设备锁、水位去重、OCC、重放恢复、在线判定
├── telemetry.py # 解析校验 + 可区分拒绝原因 + 受理
├── ota.py       # 批次约束、版本门限、状态机、超时扫描
├── service.py   # 装配与重启恢复编排
├── api.py       # FastAPI 路由与错误码映射
└── util.py      # 时间与版本号工具
tests/           # 17 个可复现单测，日志含关键输入与判定依据
```
