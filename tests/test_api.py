"""HTTP 接口测试：ASGI in-process + fakeredis + 脚本化下游。"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

import httpx
import pytest
import redis.asyncio as redis

from tests.conftest import SingleConnectionFakeRedis
from qwsmoothq.app import create_app
from qwsmoothq.pacer import AdaptivePacer
from qwsmoothq.queue import MessageQueue
from qwsmoothq.stats import now_bucket


@pytest.fixture
def patched_redis(monkeypatch):
    """把应用里的 redis 连接替换成 fakeredis。"""
    fake = SingleConnectionFakeRedis(decode_responses=True)

    def _from_url(*args, **kwargs):
        return fake

    monkeypatch.setattr(redis, "from_url", _from_url)
    return fake


@pytest.fixture(autouse=True)
async def _reset_state(patched_redis, settings):
    """每个测试前清空队列与均速器状态，避免用例间互相污染。"""
    await MessageQueue(patched_redis, settings).clear_all()
    await AdaptivePacer(patched_redis, settings).reset()


@asynccontextmanager
async def _app_client(settings, transport):
    """手动驱动 ASGI lifespan，确保后台消费者随应用启动/停止。"""
    app = create_app(settings, transport=transport)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            yield client, app


@pytest.fixture
def ok_transport():
    class OkTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request):
            return httpx.Response(200, json={"errcode": 0, "errmsg": "ok"})

    return OkTransport()


@pytest.mark.asyncio
async def test_full_http_flow(patched_redis, settings, ok_transport):
    async with _app_client(settings, ok_transport) as (client, app):
        # health
        resp = await client.get("/health")
        assert resp.status_code == 200

        # 投递
        resp = await client.post(
            "/messages",
            json={
                "api": "message/send",
                "url": "http://downstream.example/cgi-bin/message/send",
                "body": {"text": "hello"},
                "max_retries": 2,
            },
        )
        assert resp.status_code == 202
        message_id = resp.json()["message_id"]

        # 等待消费完成
        state = ""
        for _ in range(100):
            resp = await client.get(f"/messages/{message_id}")
            state = resp.json()["state"]
            if state == "succeeded":
                break
            await asyncio.sleep(0.05)
        assert state == "succeeded"

        # 速率/积压视图
        resp = await client.get("/rates")
        assert resp.status_code == 200
        assert "global" in resp.json()["rates"]
        assert resp.json()["total_backlog"] == 0

        # 直接驱动一次 tick，强制把消费者内存计数器刷入统计库（不依赖墙钟）
        await app.state.consumer.run_tick_once()

        # 按秒统计
        bucket = now_bucket()
        resp = await client.get(
            "/stats",
            params={"scope": "global", "start": bucket - 2, "end": bucket + 1},
        )
        assert resp.status_code == 200
        points = resp.json()
        assert points and sum(p["requested"] for p in points) >= 1

        resp = await client.get("/stats", params={"scope": "message/send", "seconds": 60})
        assert sum(p["requested"] for p in resp.json()) >= 1


@pytest.mark.asyncio
async def test_message_not_found_and_validation(patched_redis, settings, ok_transport):
    async with _app_client(settings, ok_transport) as (client, _app):
        resp = await client.get("/messages/nope")
        assert resp.status_code == 404

        resp = await client.post(
            "/messages",
            json={"api": "", "url": "http://x.example/y", "max_retries": -1},
        )
        assert resp.status_code == 422


@pytest.mark.asyncio
async def test_dlq_and_requeue_via_http(patched_redis, settings):
    class AlwaysLimited(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request):
            return httpx.Response(200, json={"errcode": 45009, "errmsg": "limited"})

    async with _app_client(settings, AlwaysLimited()) as (client, _app):
        resp = await client.post(
            "/messages",
            json={
                "api": "limited-api",
                "url": "http://downstream.example/cgi-bin/limited-api",
                "max_retries": 1,
            },
        )
        message_id = resp.json()["message_id"]

        state = ""
        for _ in range(150):
            resp = await client.get(f"/messages/{message_id}")
            state = resp.json()["state"]
            if state == "dead":
                break
            await asyncio.sleep(0.05)
        assert state == "dead"

        resp = await client.get("/dlq")
        assert resp.status_code == 200
        assert len(resp.json()) == 1

        resp = await client.post(f"/dlq/{message_id}/requeue")
        assert resp.status_code == 200
        assert (await client.get(f"/messages/{message_id}")).json()["state"] == "queued"

        assert (await client.post("/dlq/missing/requeue")).status_code == 404


@pytest.mark.asyncio
async def test_stats_bad_range(patched_redis, settings, ok_transport):
    async with _app_client(settings, ok_transport) as (client, _app):
        resp = await client.get(
            "/stats", params={"scope": "global", "start": 200, "end": 100}
        )
        assert resp.status_code == 400
