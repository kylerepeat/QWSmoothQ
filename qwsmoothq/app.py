"""FastAPI 应用：投递、查询、统计接口 + 后台消费者生命周期。"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

import redis.asyncio as redis
from fastapi import FastAPI, HTTPException, Query

from .config import Settings, get_settings
from .consumer import Consumer
from .models import (
    DeadLetter,
    EnqueueRequest,
    EnqueueResponse,
    Message,
    MessageStatus,
    StatsPoint,
)
from .pacer import AdaptivePacer
from .queue import MessageQueue
from .rules import build_rules
from .stats import build_stats, now_bucket

logger = logging.getLogger("qwsmoothq.api")


def create_app(settings: Settings | None = None, transport=None) -> FastAPI:
    """应用工厂。``transport`` 仅测试用（httpx mock transport）。"""
    settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.redis = redis.from_url(settings.redis_url, decode_responses=True)
        app.state.queue = MessageQueue(app.state.redis, settings)
        app.state.pacer = AdaptivePacer(app.state.redis, settings)
        app.state.stats = build_stats(settings.database_url)
        await app.state.stats.init()
        app.state.rules = build_rules(settings)
        app.state.consumer = Consumer(
            app.state.queue,
            app.state.pacer,
            app.state.stats,
            app.state.rules,
            settings,
            transport=transport,
        )
        await app.state.consumer.start()
        logger.info(
            "QWSmoothQ ready (redis=%s, stats=%s)",
            settings.redis_url,
            "postgres" if settings.database_url else "memory",
        )
        try:
            yield
        finally:
            await app.state.consumer.stop()
            await app.state.stats.close()
            await app.state.redis.aclose()

    app = FastAPI(title="QWSmoothQ", version="0.1.0", lifespan=lifespan)

    @app.post("/messages", response_model=EnqueueResponse, status_code=202)
    async def enqueue(req: EnqueueRequest) -> EnqueueResponse:
        msg = Message.from_request(req)
        await app.state.queue.enqueue(msg)
        logger.info("enqueued msg=%s api=%s url=%s", msg.id, msg.api, msg.url)
        return EnqueueResponse(message_id=msg.id, state="queued")

    @app.get("/messages/{message_id}", response_model=MessageStatus)
    async def get_message(message_id: str) -> MessageStatus:
        status = await app.state.queue.get_status(message_id)
        if status is None:
            raise HTTPException(status_code=404, detail="message not found")
        return status

    @app.get("/rates")
    async def get_rates() -> dict:
        apis = await app.state.queue.list_apis()
        rates = await app.state.pacer.get_all_rates(apis)
        return {
            "rates": rates,
            "backlogs": await app.state.queue.backlogs(),
            "total_backlog": await app.state.queue.total_backlog(),
        }

    @app.get("/dlq", response_model=list[DeadLetter])
    async def list_dlq(limit: int = Query(default=100, ge=1, le=1000)) -> list[DeadLetter]:
        return await app.state.queue.list_dlq(limit)

    @app.post("/dlq/{message_id}/requeue", response_model=EnqueueResponse)
    async def requeue_dlq(message_id: str) -> EnqueueResponse:
        msg = await app.state.queue.requeue_dlq(message_id)
        if msg is None:
            raise HTTPException(status_code=404, detail="dead letter not found")
        return EnqueueResponse(message_id=msg.id, state="queued")

    @app.get("/stats", response_model=list[StatsPoint])
    async def get_stats(
        scope: str = Query(..., description="global 或具体接口标识"),
        seconds: int = Query(default=300, ge=1, le=86400),
        start: int | None = Query(default=None, description="起始 Unix 秒，默认按 seconds 回溯"),
        end: int | None = Query(default=None, description="结束 Unix 秒，默认当前秒"),
    ) -> list[StatsPoint]:
        end_bucket = end if end is not None else now_bucket()
        start_bucket = start if start is not None else end_bucket - seconds
        if start_bucket > end_bucket:
            raise HTTPException(status_code=400, detail="start 必须不晚于 end")
        return await app.state.stats.query(scope, start_bucket, end_bucket)

    @app.get("/health")
    async def health() -> dict:
        try:
            await app.state.redis.ping()
            return {"status": "ok"}
        except Exception as exc:  # pragma: no cover - 运维兜底
            raise HTTPException(status_code=503, detail=f"redis unavailable: {exc}")

    return app
