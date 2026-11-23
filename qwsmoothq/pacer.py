"""自适应双层均速器。

两层速率状态（均持久化在 Redis 中，重启不丢）：

- 接口层 ``api:{api}``：每个接口独立的目标速率与令牌桶
- 全局层 ``global``：所有接口共享的总速率与令牌桶

派发一条消息必须同时拿到接口层和全局层各一个令牌（令牌桶容量固定 1，
因此令牌按目标速率匀速再生，从机制上避免同一瞬间扎堆打爆下游）。

调速策略（AIMD 风格）：

- 撞限流：该接口速率立刻乘性下调；若 ``correlation_window`` 秒内有
  ``correlation_min_apis`` 个不同接口限流，判定为全局限流，联动下调全局速率
- 超时/繁忙：不动速率，仅退避重试
- 限流冷却期内禁止任何回升；冷却期后，连续成功达到阈值则缓慢回升，
  积压超阈值则在上限内逐步提速消化积压
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass

import redis.asyncio as redis

from .config import Settings

_GLOBAL = "global"
_CAP = 1.0  # 令牌桶容量固定为 1：严格匀速，不攒突发量

# 原子尝试取令牌：先按流逝时间给两层桶补充令牌，两层都够 1 个才放行。
_ACQUIRE_LUA = """
-- tostring 包裹数值参数：部分 Redis 兼容实现（如 fakeredis）要求命令参数为字符串/整数
-- 标准令牌桶（retry-at 语义）：每次检查都按流逝时间补充并推进时钟
-- 只回写桶余量；时钟戳仅在真正取到令牌时推进，
-- 被拒则保留时间戳，避免流逝时间被这次拒绝吞掉
local function refill(bucket_key, ts_key, rate, now)
  local bucket = tonumber(redis.call('GET', bucket_key))
  local ts = tonumber(redis.call('GET', ts_key))
  if bucket == nil or ts == nil then
    -- 首次建桶：令牌初始为 0，时钟戳锚定到一个补票周期之前，
    -- 本次按 elapsed 恰好补满 1 个令牌（容量固定为 1）
    bucket = 0.0
    ts = now - (%f / rate)
    redis.call('SET', ts_key, tostring(ts))
  end
  local elapsed = now - ts
  if elapsed < 0 then elapsed = 0 end
  bucket = math.min(%f, bucket + elapsed * rate)
  redis.call('SET', bucket_key, tostring(bucket))
  return bucket
end

local api_rate = tonumber(redis.call('GET', KEYS[1]))
if api_rate == nil then
  api_rate = tonumber(ARGV[2])
  redis.call('SET', KEYS[1], tostring(api_rate))
end
local global_rate = tonumber(redis.call('GET', KEYS[4]))
if global_rate == nil then
  global_rate = tonumber(ARGV[3])
  redis.call('SET', KEYS[4], tostring(global_rate))
end

local now = tonumber(ARGV[1])
local api_bucket = refill(KEYS[2], KEYS[3], api_rate, now)
local global_bucket = refill(KEYS[5], KEYS[6], global_rate, now)
-- 两阶段：先确认两层桶都够，再统一扣减，避免单扣一层造成令牌泄漏
-- 1e-9 容差吸收浮点表示误差
if api_bucket >= 1.0 - 1e-9 and global_bucket >= 1.0 - 1e-9 then
  redis.call('INCRBYFLOAT', KEYS[2], tostring(-1.0))
  redis.call('INCRBYFLOAT', KEYS[5], tostring(-1.0))
  -- 只有取到令牌才推进两层桶的时钟戳
  redis.call('SET', KEYS[3], tostring(now))
  redis.call('SET', KEYS[6], tostring(now))
  return 1
end
return 0
""" % (_CAP, _CAP)

# 接口限流：降接口速率 + 记限流事件；窗口内不同接口数达标则联动降全局。
_REPORT_LIMIT_LUA = """
local now = tonumber(ARGV[1])
local min_rate = tonumber(ARGV[2])
local dec_pct = tonumber(ARGV[3])
local window = tonumber(ARGV[4])
local min_apis = tonumber(ARGV[5])
local api_name = ARGV[6]
local event_id = ARGV[7]

local api_rate = tonumber(redis.call('GET', KEYS[1])) or tonumber(ARGV[8])
api_rate = math.max(min_rate, api_rate * (1.0 - dec_pct))
redis.call('SET', KEYS[1], tostring(api_rate))
redis.call('SET', KEYS[3], '0')
redis.call('SET', KEYS[5], tostring(now))

redis.call('ZADD', KEYS[7], tostring(now), api_name .. ':' .. event_id)
redis.call('ZREMRANGEBYSCORE', KEYS[7], '-inf', tostring(now - window))
local members = redis.call('ZRANGE', KEYS[7], 0, -1)
local seen = {}
local distinct = 0
for _, m in ipairs(members) do
  local name = string.match(m, '^(.-):[^:]-$') or m
  if seen[name] == nil then
    seen[name] = true
    distinct = distinct + 1
  end
end

local global_decreased = 0
local global_rate = tonumber(redis.call('GET', KEYS[2])) or tonumber(ARGV[9])
if distinct >= min_apis then
  global_rate = math.max(min_rate, global_rate * (1.0 - dec_pct))
  redis.call('SET', KEYS[2], tostring(global_rate))
  redis.call('SET', KEYS[4], '0')
  redis.call('SET', KEYS[6], tostring(now))
  global_decreased = 1
end
return {tostring(api_rate), tostring(global_rate), distinct, global_decreased}
"""

# 连续成功后的缓慢回升（冷却期内不回升）。
_REPORT_SUCCESS_LUA = """
local now = tonumber(ARGV[1])
local max_rate = tonumber(ARGV[2])
local recovery_pct = tonumber(ARGV[3])
local streak_needed = tonumber(ARGV[4])
local cooldown = tonumber(ARGV[5])

local last_limit = tonumber(redis.call('GET', KEYS[3]))
if last_limit ~= nil and now - last_limit < cooldown then
  return tostring(tonumber(redis.call('GET', KEYS[1])) or 0.0)
end
local streak = redis.call('INCR', KEYS[2])
local rate = tonumber(redis.call('GET', KEYS[1])) or 0.0
if streak >= streak_needed then
  rate = math.min(max_rate, rate * (1.0 + recovery_pct))
  redis.call('SET', KEYS[1], tostring(rate))
  redis.call('SET', KEYS[2], '0')
end
return tostring(rate)
"""

# 每秒调速 tick：积压超阈值且不在冷却期 => 在上限内乘性提速。
_TICK_LUA = """
local now = tonumber(ARGV[1])
local backlog = tonumber(ARGV[2])
local threshold = tonumber(ARGV[3])
local inc_pct = tonumber(ARGV[4])
local max_rate = tonumber(ARGV[5])
local cooldown = tonumber(ARGV[6])

local rate = tonumber(redis.call('GET', KEYS[1])) or tonumber(ARGV[7])
local last_limit = tonumber(redis.call('GET', KEYS[3]))
local in_cooldown = last_limit ~= nil and now - last_limit < cooldown
if backlog > threshold and not in_cooldown then
  rate = math.min(max_rate, rate * (1.0 + inc_pct))
  redis.call('SET', KEYS[1], tostring(rate))
  redis.call('SET', KEYS[2], '0')
end
return tostring(rate)
"""


@dataclass(frozen=True)
class LimitDecision:
    api_rate: float
    global_rate: float
    distinct_apis: int
    global_decreased: bool


class AdaptivePacer:
    def __init__(self, client: redis.Redis, settings: Settings) -> None:
        self._redis = client
        self._s = settings
        p = settings.redis_key_prefix
        self._p = p
        self._acquire = self._redis.register_script(_ACQUIRE_LUA)
        self._report_limit = self._redis.register_script(_REPORT_LIMIT_LUA)
        self._report_success = self._redis.register_script(_REPORT_SUCCESS_LUA)
        self._tick = self._redis.register_script(_TICK_LUA)

    # ---------- key 规划 ----------

    def _rate(self, scope: str) -> str:
        return f"{self._p}:rate:{scope}"

    def _bucket(self, scope: str) -> str:
        return f"{self._p}:bucket:{scope}"

    def _bucket_ts(self, scope: str) -> str:
        return f"{self._p}:bucket:{scope}:ts"

    def _streak(self, scope: str) -> str:
        return f"{self._p}:streak:{scope}"

    def _last_limit(self, scope: str) -> str:
        return f"{self._p}:lastlimit:{scope}"

    @property
    def _events_key(self) -> str:
        return f"{self._p}:limit_events"

    def _api_scope(self, api: str) -> str:
        return f"api:{api}"

    # ---------- 派发 ----------

    async def acquire(self, api: str, now: float | None = None) -> bool:
        """两层令牌桶各取一个令牌，全部成功才放行。"""
        now = time.time() if now is None else now
        api_scope = self._api_scope(api)
        result = await self._acquire(
            keys=[
                self._rate(api_scope),
                self._bucket(api_scope),
                self._bucket_ts(api_scope),
                self._rate(_GLOBAL),
                self._bucket(_GLOBAL),
                self._bucket_ts(_GLOBAL),
            ],
            args=[now, self._s.initial_rate, self._s.initial_rate],
        )
        return int(result) == 1

    # ---------- 反馈 ----------

    async def report_api_limit(self, api: str, now: float | None = None) -> LimitDecision:
        now = time.time() if now is None else now
        api_scope = self._api_scope(api)
        api_rate, global_rate, distinct, global_decreased = await self._report_limit(
            keys=[
                self._rate(api_scope),
                self._rate(_GLOBAL),
                self._streak(api_scope),
                self._streak(_GLOBAL),
                self._last_limit(api_scope),
                self._last_limit(_GLOBAL),
                self._events_key,
            ],
            args=[
                now,
                self._s.min_rate,
                self._s.rate_decrease_pct,
                self._s.limit_correlation_window,
                self._s.limit_correlation_min_apis,
                api,
                uuid.uuid4().hex,
                self._s.initial_rate,
                self._s.initial_rate,
            ],
        )
        return LimitDecision(
            api_rate=float(api_rate),
            global_rate=float(global_rate),
            distinct_apis=int(distinct),
            global_decreased=bool(global_decreased),
        )

    async def report_success(self, scope: str, now: float | None = None) -> float:
        """连续成功达到阈值后缓慢回升。scope 为 'global' 或 'api:xxx'。"""
        now = time.time() if now is None else now
        max_rate = self._s.global_max_rate if scope == _GLOBAL else self._s.max_rate
        rate = await self._report_success(
            keys=[self._rate(scope), self._streak(scope), self._last_limit(scope)],
            args=[
                now,
                max_rate,
                self._s.recovery_increase_pct,
                self._s.success_streak_needed,
                self._s.limit_correlation_window,
            ],
        )
        return float(rate)

    async def report_api_success(self, api: str, now: float | None = None) -> None:
        now = time.time() if now is None else now
        await self.report_success(self._api_scope(api), now)
        await self.report_success(_GLOBAL, now)

    async def tick(
        self, scope: str, backlog: int, max_rate: float, now: float | None = None
    ) -> float:
        """每秒一次的调速 tick，返回该 scope 当前目标速率。"""
        now = time.time() if now is None else now
        rate = await self._tick(
            keys=[self._rate(scope), self._streak(scope), self._last_limit(scope)],
            args=[
                now,
                backlog,
                self._s.backlog_threshold,
                self._s.rate_increase_pct,
                max_rate,
                self._s.limit_correlation_window,
                self._s.initial_rate,
            ],
        )
        return float(rate)

    # ---------- 查询 ----------

    async def get_rate(self, scope: str) -> float:
        value = await self._redis.get(self._rate(scope))
        return float(value) if value is not None else self._s.initial_rate

    async def get_all_rates(self, apis: list[str] | None = None) -> dict[str, float]:
        api_rate_keys = [self._rate(self._api_scope(api)) for api in (apis or [])]
        values = await self._redis.mget([self._rate(_GLOBAL), *api_rate_keys])
        result = {_GLOBAL: float(values[0]) if values[0] is not None else self._s.initial_rate}
        for api, value in zip(apis or [], values[1:]):
            result[self._api_scope(api)] = (
                float(value) if value is not None else self._s.initial_rate
            )
        return result

    async def reset(self) -> None:
        """清掉均速器相关 key（测试用）。"""
        async for key in self._redis.scan_iter(match=f"{self._p}:rate:*"):
            await self._redis.delete(key)
        async for key in self._redis.scan_iter(match=f"{self._p}:bucket:*"):
            await self._redis.delete(key)
        async for key in self._redis.scan_iter(match=f"{self._p}:streak:*"):
            await self._redis.delete(key)
        async for key in self._redis.scan_iter(match=f"{self._p}:lastlimit:*"):
            await self._redis.delete(key)
        await self._redis.delete(self._events_key)
