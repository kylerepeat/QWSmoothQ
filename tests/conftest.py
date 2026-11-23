"""pytest 公共 fixtures：fakeredis、内存统计、可编排返回的 httpx transport。"""

from __future__ import annotations

import json
from collections import deque

import fakeredis.aioredis
import httpx
import pytest
import pytest_asyncio

from qwsmoothq.config import Settings
from qwsmoothq.pacer import AdaptivePacer
from qwsmoothq.queue import MessageQueue
from qwsmoothq.stats import MemoryStats


class SingleConnectionFakeRedis(fakeredis.aioredis.FakeRedis):
    """测试专用 FakeRedis。

    fakeredis 把 SCRIPT LOAD 缓存按连接隔离，而 redis-py 连接池每次可能取到
    不同连接，EVALSHA 重试仍会落到没有脚本缓存的连接上（真实 Redis 的脚本
    缓存是服务端全局的，无此问题）。这里让 register_script 直接返回一个
    始终走 EVAL 的可调用对象，彻底绕开脚本缓存差异。
    """

    def register_script(self, script):
        async def _invoke(keys=None, args=None, **_kwargs):
            return await self.eval(script, len(keys or []), *(keys or []), *(args or []))

        return _invoke


@pytest.fixture
def settings() -> Settings:
    # 调小阈值与冷却窗口，测试里更快观察到调速行为
    return Settings(
        redis_url="redis://localhost:6379/0",
        database_url="",
        initial_rate=10.0,
        min_rate=0.5,
        max_rate=20.0,
        global_max_rate=50.0,
        backlog_threshold=5,
        rate_increase_pct=0.2,
        rate_decrease_pct=0.5,
        recovery_increase_pct=0.05,
        success_streak_needed=3,
        limit_correlation_window=10,
        limit_correlation_min_apis=2,
        backoff_base=0.05,
        backoff_factor=2.0,
        backoff_max=1.0,
        backoff_jitter=0.1,
    )


@pytest_asyncio.fixture(scope="function")
async def redis_client():
    # 函数级新实例，保证测试间状态完全隔离
    client = SingleConnectionFakeRedis(decode_responses=True)
    yield client
    await client.flushall()
    await client.aclose()


@pytest_asyncio.fixture
async def queue(redis_client, settings) -> MessageQueue:
    q = MessageQueue(redis_client, settings)
    await q.clear_all()
    return q


@pytest_asyncio.fixture
async def pacer(redis_client, settings) -> AdaptivePacer:
    p = AdaptivePacer(redis_client, settings)
    await p.reset()
    return p


@pytest.fixture
def stats() -> MemoryStats:
    return MemoryStats()


class ScriptedTransport(httpx.AsyncBaseTransport):
    """按排队脚本返回响应的 httpx transport，测试中代替真实下游。"""

    def __init__(self) -> None:
        self.script: deque = deque()
        self.calls: list[httpx.Request] = []

    def push_response(self, status_code: int = 200, json_body: dict | None = None) -> None:
        self.script.append(("response", status_code, json_body))

    def push_exception(self, exc: Exception) -> None:
        self.script.append(("exception", exc))

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.calls.append(request)
        if not self.script:
            return httpx.Response(200, json={"errcode": 0, "errmsg": "ok"})
        kind, *payload = self.script.popleft()
        if kind == "exception":
            raise payload[0]
        status_code, json_body = payload
        return httpx.Response(
            status_code,
            content=json.dumps(json_body).encode(),
            headers={"content-type": "application/json"},
        )


@pytest.fixture
def scripted_transport() -> ScriptedTransport:
    return ScriptedTransport()
