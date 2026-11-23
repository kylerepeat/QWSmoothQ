"""端到端集成：积压提速、撞限降频、重试成功、多接口联动与按秒统计。

不依赖外部 Redis/PostgreSQL，全部用 fakeredis + 内存统计 + 脚本化下游，
但走的是与生产一致的 Consumer/Pacer/Queue/Stats 真实代码路径。
"""

from __future__ import annotations

import asyncio

import httpx
import pytest

from qwsmoothq.consumer import Consumer
from qwsmoothq.models import EnqueueRequest, Message
from qwsmoothq.rules import build_rules
from qwsmoothq.stats import MemoryStats, now_bucket


def _build(settings, queue, pacer, stats, transport) -> Consumer:
    return Consumer(
        queue=queue,
        pacer=pacer,
        stats=stats,
        rules=build_rules(settings),
        settings=settings,
        transport=transport,
    )


def _msg(api: str, max_retries: int = 5) -> Message:
    return Message.from_request(
        EnqueueRequest(
            api=api,
            url=f"http://downstream.example/cgi-bin/{api}",
            body={"api": api},
            max_retries=max_retries,
        )
    )


class SequenceTransport(httpx.AsyncBaseTransport):
    """前 limit_count 次返回限流，之后全部成功。"""

    def __init__(self, limit_count: int) -> None:
        self.limit_count = limit_count
        self.calls = 0

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.calls += 1
        if self.calls <= self.limit_count:
            return httpx.Response(200, json={"errcode": 45009, "errmsg": "limited"})
        return httpx.Response(200, json={"errcode": 0, "errmsg": "ok"})


@pytest.mark.asyncio
async def test_backlog_ramp_then_limit_cut_then_retry_and_stats(
    queue, pacer, settings
):
    # 1) 先造一批积压（不启动消费者），验证 tick 在上限内持续提速
    for _ in range(12):
        await queue.enqueue(_msg("ramp-api"))
    initial = await pacer.get_rate("api:ramp-api")
    for _ in range(5):
        await pacer.tick("api:ramp-api", backlog=12, max_rate=settings.max_rate)
    ramped = await pacer.get_rate("api:ramp-api")
    assert ramped > initial
    assert ramped <= settings.max_rate

    # 2) 启动消费者，前 3 次撞限流后成功：验证降频 + 退避重试成功
    transport = SequenceTransport(limit_count=3)
    stats = MemoryStats()
    consumer = _build(settings, queue, pacer, stats, transport)
    await consumer.start()
    try:
        # 等积压被全部消化
        backlog_trace = []
        deadline = asyncio.get_running_loop().time() + 6
        while asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.1)
            backlog = await queue.total_backlog()
            backlog_trace.append(backlog)
            if backlog == 0:
                break
        assert backlog_trace[-1] == 0, "积压应被全部消化"
    finally:
        await consumer.stop()

    # 撞过限流 => 接口速率被压到初始值以下（冷却期内不会回升）
    assert await pacer.get_rate("api:ramp-api") < initial
    # 12 条消息最终全部成功，且限流的 3 条经历了重试
    assert transport.calls >= 12

    # 3) 统计可查：全局与接口维度都有请求/限流/重试记录（处理可能跨秒，宽窗查询）
    await consumer.run_tick_once()
    end_bucket = now_bucket()
    global_points = await stats.query("global", end_bucket - 30, end_bucket + 1)
    api_points = await stats.query("ramp-api", end_bucket - 30, end_bucket + 1)
    # requested 按派发次数计：12 条首发 + 3 次限流重试 = 15
    assert sum(p.requested for p in global_points) == 15
    assert sum(p.requested for p in api_points) == 15
    assert sum(p.limited for p in api_points) == 3
    assert sum(p.retried for p in api_points) == 3
    assert sum(p.succeeded for p in api_points) == 12
    # 采样点带积压与速率字段
    assert any(p.rate is not None for p in api_points)


@pytest.mark.asyncio
async def test_two_apis_limiting_together_cuts_global(queue, pacer, settings):
    transport = SequenceTransport(limit_count=2)
    stats = MemoryStats()
    consumer = _build(settings, queue, pacer, stats, transport)

    global_before = await pacer.get_rate("global")
    await queue.enqueue(_msg("api-a"))
    await queue.enqueue(_msg("api-b"))

    await consumer.start()
    try:
        deadline = asyncio.get_running_loop().time() + 6
        while asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.1)
            if await queue.total_backlog() == 0:
                break
        assert await queue.total_backlog() == 0
    finally:
        await consumer.stop()

    # 两个不同接口在关联窗口内各撞一次限流 => 全局速率被联动下调
    assert await pacer.get_rate("global") < global_before
