"""Redis 维护的消息队列。

结构（key 前缀由 ``redis_key_prefix`` 决定，默认 ``qwsmoothq``）：

- ``ready:{api}``  List  各接口的就绪队列，LPUSH 入队 / RPOP 出队（FIFO）
- ``delayed``      ZSet  延迟队列，score = 到期 Unix 时间戳，退避重试的消息放这里
- ``dlq``          List  死信队列，超过最大重试次数的消息连带失败原因
- ``apis``         Set   出现过的接口标识，供消费者轮询调度
- ``backlog:{api}`` String 每接口积压计数（ready + delayed，不含在途）
- ``msg:{id}``     Hash  消息状态，带 TTL，供状态查询
- ``dlq:idx:{id}`` String 死信标记，防重复入死信
"""

from __future__ import annotations

import time
from typing import Any

import redis.asyncio as redis

from .config import Settings
from .models import DeadLetter, Message, MessageState


class QueueKeys:
    def __init__(self, prefix: str) -> None:
        self.prefix = prefix
        self.ready_prefix = f"{prefix}:ready:"
        self.delayed = f"{prefix}:delayed"
        self.dlq = f"{prefix}:dlq"
        self.apis = f"{prefix}:apis"
        self.backlog_prefix = f"{prefix}:backlog:"
        self.msg_prefix = f"{prefix}:msg:"
        self.dlq_idx_prefix = f"{prefix}:dlq:idx:"

    def ready(self, api: str) -> str:
        return f"{self.ready_prefix}{api}"

    def backlog(self, api: str) -> str:
        return f"{self.backlog_prefix}{api}"

    def msg(self, message_id: str) -> str:
        return f"{self.msg_prefix}{message_id}"

    def dlq_idx(self, message_id: str) -> str:
        return f"{self.dlq_idx_prefix}{message_id}"


# 把到期的延迟消息原子地搬回各自的就绪队列，并恢复积压计数。
_MOVE_DUE_LUA = """
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
for _, raw in ipairs(due) do
    local ok, msg = pcall(cjson.decode, raw)
    if ok and msg['api'] ~= nil then
        redis.call('LPUSH', ARGV[2] .. msg['api'], raw)
        redis.call('INCR', ARGV[3] .. msg['api'])
        redis.call('SADD', KEYS[2], msg['api'])
    end
end
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
return #due
"""


class MessageQueue:
    def __init__(self, client: redis.Redis, settings: Settings) -> None:
        self._redis = client
        self._settings = settings
        self.keys = QueueKeys(settings.redis_key_prefix)
        self._move_due = self._redis.register_script(_MOVE_DUE_LUA)
        self._cursor = 0

    # ---------- 投递 ----------

    async def enqueue(self, msg: Message) -> None:
        raw = msg.model_dump_json()
        async with self._redis.pipeline(transaction=True) as pipe:
            pipe.lpush(self.keys.ready(msg.api), raw)
            pipe.incr(self.keys.backlog(msg.api))
            pipe.sadd(self.keys.apis, msg.api)
            await pipe.execute()
        await self.write_status(
            msg,
            state="queued",
            error=None,
            response_status=None,
            response_body=None,
        )

    # ---------- 调度 ----------

    async def move_due(self, now: float | None = None) -> int:
        """把到期的延迟消息搬回就绪队列，返回搬运条数。"""
        now = time.time() if now is None else now
        return int(
            await self._move_due(
                keys=[self.keys.delayed, self.keys.apis],
                args=[now, self.keys.ready_prefix, self.keys.backlog_prefix],
            )
        )

    async def list_apis(self) -> list[str]:
        return sorted(await self._redis.smembers(self.keys.apis))

    async def pop_ready_round_robin(self) -> tuple[str | None, Message | None]:
        """轮询各接口就绪队列，弹出一条消息；空队列返回 (None, None)。"""
        apis = await self.list_apis()
        if not apis:
            return None, None
        n = len(apis)
        for step in range(n):
            api = apis[(self._cursor + step) % n]
            raw = await self._redis.rpop(self.keys.ready(api))
            if raw is not None:
                self._cursor = (self._cursor + step + 1) % n
                await self._redis.decr(self.keys.backlog(api))
                return api, Message.model_validate_json(raw)
        return None, None

    async def return_head(self, api: str, msg: Message) -> None:
        """均速器拒绝放行时，把弹出的消息放回队首并恢复积压计数。"""
        async with self._redis.pipeline(transaction=True) as pipe:
            pipe.lpush(self.keys.ready(api), msg.model_dump_json())
            pipe.incr(self.keys.backlog(api))
            await pipe.execute()

    async def delay_for_retry(self, msg: Message, due_ts: float) -> None:
        """放入延迟队列；到期由 move_due 搬回。"""
        await self._redis.zadd(self.keys.delayed, {msg.model_dump_json(): due_ts})

    # ---------- 死信 ----------

    async def send_to_dlq(
        self,
        msg: Message,
        reason: str,
        response_status: int | None,
        response_body: str | None,
    ) -> DeadLetter:
        dl = DeadLetter(
            message=msg,
            reason=reason,
            response_status=response_status,
            response_body=response_body,
            dead_at=time.time(),
        )
        marker = self.keys.dlq_idx(msg.id)
        # SET NX 成功才是首次进 DLQ；已存在标记则不重复入队
        marker_set = await self._redis.set(marker, "1", nx=True)
        if not marker_set:
            return dl
        # 兜底清理：从延迟集合/就绪队列移除任何残留，保证消息进 DLQ 后不再计入积压。
        # 正常消费路径下消息已从就绪队列弹出（LREM 删 0 条），不受影响。
        raw = msg.model_dump_json()
        await self._redis.zrem(self.keys.delayed, raw)
        removed_from_ready = await self._redis.lrem(self.keys.ready(msg.api), 0, raw)
        await self._redis.lpush(self.keys.dlq, dl.model_dump_json())
        if removed_from_ready:
            await self._redis.eval(
                "local v = tonumber(redis.call('GET', KEYS[1]) or '0') "
                "if v > 0 then redis.call('DECR', KEYS[1]) end",
                1,
                self.keys.backlog(msg.api),
            )
        await self.write_status(msg, "dead", reason, response_status, response_body)
        return dl

    async def list_dlq(self, limit: int = 100) -> list[DeadLetter]:
        raws = await self._redis.lrange(self.keys.dlq, 0, limit - 1)
        return [DeadLetter.model_validate_json(raw) for raw in raws]

    async def requeue_dlq(self, message_id: str) -> Message | None:
        """把死信重新投入就绪队列（手动补救用），重试计数清零。"""
        for index, raw in enumerate(await self._redis.lrange(self.keys.dlq, 0, -1)):
            dl = DeadLetter.model_validate_json(raw)
            if dl.message.id != message_id:
                continue
            msg = dl.message.model_copy(update={"attempt": 0})
            async with self._redis.pipeline(transaction=True) as pipe:
                pipe.lrem(self.keys.dlq, 1, raw)
                pipe.delete(self.keys.dlq_idx(message_id))
                pipe.lpush(self.keys.ready(msg.api), msg.model_dump_json())
                pipe.incr(self.keys.backlog(msg.api))
                pipe.sadd(self.keys.apis, msg.api)
                await pipe.execute()
            await self.write_status(msg, "queued", None, None, None)
            return msg
        return None

    # ---------- 状态与积压 ----------

    async def write_status(
        self,
        msg: Message,
        state: MessageState,
        error: str | None,
        response_status: int | None,
        response_body: str | None,
    ) -> None:
        key = self.keys.msg(msg.id)
        mapping: dict[str, Any] = {
            "id": msg.id,
            "api": msg.api,
            "state": state,
            "attempt": str(msg.attempt),
            "max_retries": str(msg.max_retries),
            "enqueued_at": str(msg.enqueued_at),
            "updated_at": str(time.time()),
            "last_error": error or "",
            "response_status": "" if response_status is None else str(response_status),
            "response_body": "" if response_body is None else response_body[:4000],
        }
        ttl = self._settings.status_ttl_hours * 3600
        async with self._redis.pipeline(transaction=True) as pipe:
            pipe.hset(key, mapping=mapping)
            pipe.expire(key, ttl)
            await pipe.execute()

    async def get_status(self, message_id: str) -> MessageStatus | None:
        from .models import MessageStatus  # 局部导入避免循环语义歧义

        data = await self._redis.hgetall(self.keys.msg(message_id))
        if not data:
            return None
        return MessageStatus(
            id=data["id"],
            api=data["api"],
            state=data["state"],
            attempt=int(data["attempt"]),
            max_retries=int(data["max_retries"]),
            enqueued_at=float(data["enqueued_at"]),
            updated_at=float(data["updated_at"]),
            last_error=data.get("last_error") or None,
            response_status=int(data["response_status"]) if data.get("response_status") else None,
            response_body=data.get("response_body") or None,
        )

    async def backlog(self, api: str) -> int:
        value = await self._redis.get(self.keys.backlog(api))
        return int(value) if value else 0

    async def total_backlog(self) -> int:
        apis = await self.list_apis()
        if not apis:
            return 0
        values = await self._redis.mget([self.keys.backlog(api) for api in apis])
        return sum(int(v) for v in values if v)

    async def backlogs(self) -> dict[str, int]:
        apis = await self.list_apis()
        if not apis:
            return {}
        values = await self._redis.mget([self.keys.backlog(api) for api in apis])
        return {api: int(value) if value else 0 for api, value in zip(apis, values)}

    async def clear_all(self) -> None:
        """清空该前缀下的所有 key（测试/重置用）。"""
        async for key in self._redis.scan_iter(match=f"{self.keys.prefix}:*"):
            await self._redis.delete(key)
        self._cursor = 0
