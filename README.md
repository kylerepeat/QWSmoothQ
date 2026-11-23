# QWSmoothQ

面向**企业微信这类限流规则不透明的外部 API** 的自适应均速队列中间件。业务方不直接调下游，而是把请求消息投进中间件的 HTTP 接口；中间件在后台按"积压提速、撞限降频"的节奏异步消费、转发、重试，并按秒记录速率与限流数据，供后续摸清下游限流规律。

## 工作机制

```
业务方 ──POST /messages──▶ [Redis 就绪队列(按接口分队列)]
                                │  双层令牌桶(接口层 + 全局层)匀速放行
                                ▼
                          后台消费者 ──HTTP──▶ 下游(企微等)
                                │
        ┌───────────────────────┼────────────────────────┐
        ▼                       ▼                        ▼
  成功: 连续成功缓慢回升    限流信号: 立即乘性降速     超时/繁忙: 退避重试
        (统计落 PostgreSQL) 多接口同窗限流→降全局      超限→死信队列
```

- **消息队列（Redis）**：每接口一个就绪 List（LPUSH/RPOP，FIFO），退避重试走延迟 ZSET，超限消息进死信 List；消息状态 Hash 带 TTL，可按 ID 查询。
- **自适应均速器（Redis 状态 + Lua 原子操作）**：
  - 接口层与全局层各维护一个令牌桶，桶容量固定为 1，令牌按目标速率匀速再生 → 天然避免瞬间扎堆；
  - 积压超过阈值且不在冷却期 → 在上限内每秒乘性提速消化积压；
  - 识别到限流信号 → 该接口速率立即乘性下调；关联窗口内多个**不同接口**同时限流 → 判定全局限流，联动下调全局速率；
  - 限流冷却期内禁止任何回升；冷却期后连续成功达到阈值才缓慢回升（AIMD）。
- **可插拔限流规则**：内置 HTTP 429 与企业微信返回体规则。企微接口通常 HTTP 200、用 JSON `errcode` 表达错误，默认把 `45009`（接口调用超过限制）、`45011`（接口速度限制）判为限流，`-1`（系统繁忙）判为可重试的临时失败；**具体错误码语义请以[企业微信开发者文档](https://developer.work.weixin.qq.com/document/)为准**，可通过环境变量调整，也可实现 `qwsmoothq.rules.LimitRule` 接口后用 `"模块路径:类名"` 插入自定义规则。
- **退避重试**：带 ±抖动的指数退避（`base * factor^(attempt-1) + jitter`），重试消息进延迟队列，超过 `max_retries` 进死信队列并保留失败原因，支持查询与手动重投。
- **按秒统计（PostgreSQL）**：全局和每接口的实际请求数、成功数、限流次数、重试次数、死信数、队列积压量、生效速率；未配置 `QWSQ_DATABASE_URL` 时自动降级为内存统计（重启丢失，便于本地试玩与测试）。

## 目录结构

```
qwsmoothq/          中间件主包（config/rules/queue/pacer/stats/consumer/app/cli）
mock_wecom/         本地 Mock 服务，模拟企微返回体与单接口/全局限流
tests/              pytest 测试（fakeredis + httpx mock，无需外部服务）
scripts/load_demo.py 批量灌消息脚本
docker-compose.yml  本地 Redis + PostgreSQL
```

## 本地启动（验收流程）

前置：Python 3.14、Poetry（2.2.1+）、Redis；统计落库还需要 PostgreSQL（可选）。

```bash
# 1. 安装依赖
poetry install

# 2. 启动基础设施（二选一：有 Docker 用 compose；没有就用本机 Redis）
docker compose up -d
cp .env.example .env        # 不想用 PostgreSQL 就把 QWSQ_DATABASE_URL 留空

# 3. 启动 Mock 企微服务（端口 9000）
poetry run mock-wecom

# 4. 另开终端启动中间件（端口 8000，含后台消费者）
poetry run qwsmoothq

# 5. 再开一个终端，灌一批消息制造积压（默认 100 条、两个接口轮发）
poetry run python scripts/load_demo.py --n 200
```

### 制造限流，观察降频与重试

```bash
# 单接口限流：message/send 超过 2 rps 即返回 errcode=45009
curl -X POST http://127.0.0.1:9000/mock/control/limit \
  -H 'content-type: application/json' \
  -d '{"api":"message/send","rps":2}'

# 全局限流：所有接口共享 5 rps 上限
curl -X POST http://127.0.0.1:9000/mock/control/limit \
  -H 'content-type: application/json' \
  -d '{"api":"*","rps":5}'

# 解除限流
curl -X POST http://127.0.0.1:9000/mock/control/limit \
  -H 'content-type: application/json' \
  -d '{"api":"message/send","rps":null}'
```

### 观察运行状态

```bash
# 实时速率（global + 每接口）与积压
curl http://127.0.0.1:8000/rates

# 单条消息状态（queued/running/retrying/succeeded/dead）
curl http://127.0.0.1:8000/messages/<message_id>

# 死信队列 / 手动重投
curl 'http://127.0.0.1:8000/dlq?limit=50'
curl -X POST http://127.0.0.1:8000/dlq/<message_id>/requeue

# 按秒统计（全局 / 单接口），可指定 seconds 或 start/end（Unix 秒）
curl 'http://127.0.0.1:8000/stats?scope=global&seconds=300'
curl 'http://127.0.0.1:8000/stats?scope=message/send&seconds=300'
```

中间件日志会直接打印每次限流后的速率变化，例如：
`api=message/send rate limited -> api_rate=2.50 global_rate=10.00 distinct=1 global_cut=False`；
两个不同接口同窗限流时会出现 `global_cut=True`。

## HTTP 接口一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/messages` | 投递消息（api、url、method、headers、params、body、max_retries） |
| GET | `/messages/{id}` | 查询消息状态与最后错误/响应摘要 |
| GET | `/rates` | 当前各层目标速率与每接口/总积压 |
| GET | `/dlq` | 死信列表 |
| POST | `/dlq/{id}/requeue` | 死信重新入队（重试计数清零） |
| GET | `/stats?scope=&seconds=` | 按秒统计点（scope 为 `global` 或接口标识） |
| GET | `/health` | 健康检查 |

## 配置项

全部配置以 `QWSQ_` 为前缀，可用环境变量或 `.env` 覆盖，见 `.env.example`。关键项：

| 配置 | 默认 | 含义 |
| --- | --- | --- |
| `INITIAL_RATE` / `MIN_RATE` / `MAX_RATE` | 5 / 0.5 / 20 | 单接口速率上下限（rps） |
| `GLOBAL_MAX_RATE` | 50 | 全局速率上限 |
| `BACKLOG_THRESHOLD` | 20 | 积压超过该值触发提速 |
| `RATE_INCREASE_PCT` / `RATE_DECREASE_PCT` | 0.2 / 0.5 | 提速/降频的乘性比例 |
| `RECOVERY_INCREASE_PCT` | 0.05 | 冷却期后连续成功的缓慢回升比例 |
| `SUCCESS_STREAK_NEEDED` | 10 | 连续成功多少次才回升 |
| `LIMIT_CORRELATION_WINDOW` / `..._MIN_APIS` | 10 秒 / 2 个 | 多接口限流联动降全局的判定窗口与数量 |
| `BACKOFF_BASE/FACTOR/MAX/JITTER` | 0.5/2/30/0.25 | 指数退避与抖动 |
| `LIMIT_RULES` | `["http429","wecom"]` | 限流规则链，支持自定义类路径 |
| `WECOM_LIMIT_ERRCODES` / `WECOM_BUSY_ERRCODES` | `[45009,45011]` / `[-1]` | 企微限流码 / 繁忙码 |

## 测试

```bash
poetry run pytest
```

测试全部基于 fakeredis（含 Lua 支持）与 httpx mock transport，不需要真实 Redis/PostgreSQL，覆盖（共 49 个用例）：规则判定、队列 FIFO/延迟/死信、双层令牌桶与 AIMD 调速/联动降频/冷却语义、消费者端到端（限流重试成功、超时重试、超限进死信、统计落库）、HTTP 接口、Mock 服务，以及一条把"积压提速 → 撞限降频 → 退避重试成功 → 多接口联动降全局 → 按秒统计可查"串起来的完整集成链路（`tests/test_integration.py`）。

## 部署备注

- 单实例即可满足绝大多数场景；如需多实例水平扩展，所有速率与队列状态都在 Redis，多消费者共享同一组令牌桶（Lua 保证原子），只需部署多个中间件实例。
- PostgreSQL 表 `per_second_stats` 由应用自动建表；长期运行建议按 `bucket` 做分区或定期归档。
