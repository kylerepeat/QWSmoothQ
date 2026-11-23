"""后台消费者：异步消费队列、按均速器节奏派发、处理下游反馈。

单 worker 设计（可水平扩多实例，速率状态在 Redis 中共享）。主循环做两件事：

1. ``_tick_loop``：每秒一次——搬运到期延迟消息、按积压执行调速 tick、
   按秒采样积压/速率并把内存计数器刷入统计库
2. ``_dispatch_loop``：轮询就绪队列，拿到消息后过双层令牌桶，放行才发请求；
   限流/超时按带抖动的指数退避塞回延迟队列，超过最大重试进死信
"""

from __future__ import annotations

import asyncio
import logging
import random
import time

import httpx

from .config import Settings
from .models import Message
from .pacer import AdaptivePacer
from .queue import MessageQueue
from .rules import detect_limit
from .stats import StatsSink, now_bucket

logger = logging.getLogger("qwsmoothq.consumer")


class Consumer:
    def __init__(
        self,
        queue: MessageQueue,
        pacer: AdaptivePacer,
        stats: StatsSink,
        rules: list,
        settings: Settings,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._queue = queue
        self._pacer = pacer
        self._stats = stats
        self._rules = rules
        self._s = settings
        self._transport = transport
        self._client: httpx.AsyncClient | None = None
        self._stopping = asyncio.Event()
        self._tasks: list[asyncio.Task] = []
        # 当前秒的内存计数器，每秒由 tick 刷入统计库
        self._counters: dict[tuple[int, str], dict[str, int]] = {}
        self._last_tick_bucket: int | None = None

    # ---------- 生命周期 ----------

    async def start(self) -> None:
        self._client = httpx.AsyncClient(
            timeout=self._s.http_timeout, transport=self._transport
        )
        self._stopping.clear()
        self._tasks = [
            asyncio.create_task(self._tick_loop(), name="qwsmoothq-tick"),
            asyncio.create_task(self._dispatch_loop(), name="qwsmoothq-dispatch"),
        ]
        logger.info("consumer started")

    async def stop(self) -> None:
        self._stopping.set()
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
        if self._client is not None:
            await self._client.aclose()
        await self._flush_counters()
        logger.info("consumer stopped")

    # ---------- 每秒调速与统计 ----------

    async def run_tick_once(self) -> None:
        """执行一次每秒 tick（测试可直接调用）。"""
        moved = await self._queue.move_due()
        if moved:
            logger.debug("moved %d due messages back to ready queues", moved)

        bucket = now_bucket()
        await self._flush_counters()

        backlogs = await self._queue.backlogs()
        total = sum(backlogs.values())

        global_rate = await self._pacer.tick(
            "global", total, self._s.global_max_rate, now=float(bucket)
        )
        await self._stats.record_backlog_and_rate(bucket, "global", total, global_rate)

        for api, backlog in backlogs.items():
            scope = f"api:{api}"
            api_rate = await self._pacer.tick(
                scope, backlog, self._s.max_rate, now=float(bucket)
            )
            await self._stats.record_backlog_and_rate(bucket, api, backlog, api_rate)

        self._last_tick_bucket = bucket

    async def _tick_loop(self) -> None:
        while not self._stopping.is_set():
            try:
                await self.run_tick_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("tick loop error")
            try:
                await asyncio.wait_for(self._stopping.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

    # ---------- 派发主循环 ----------

    async def _dispatch_loop(self) -> None:
        while not self._stopping.is_set():
            try:
                api, msg = await self._queue.pop_ready_round_robin()
                if msg is None or api is None:
                    await self._short_sleep()
                    continue
                allowed = await self._pacer.acquire(api)
                if not allowed:
                    # 令牌不够：放回队首，稍后重试（不消耗重试次数）
                    await self._queue.return_head(api, msg)
                    await self._short_sleep()
                    continue
                await self._process(api, msg)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("dispatch loop unexpected error")
                await self._short_sleep()

    async def _short_sleep(self) -> None:
        try:
            await asyncio.wait_for(self._stopping.wait(), timeout=0.05)
        except asyncio.TimeoutError:
            pass

    # ---------- 单条消息处理 ----------

    async def _process(self, api: str, msg: Message) -> None:
        msg.attempt += 1
        await self._queue.write_status(msg, "running", None, None, None)
        self._count("requested", api)

        limited = False
        transient = False
        response: httpx.Response | None = None
        error_text: str | None = None

        try:
            assert self._client is not None
            response = await self._client.request(
                msg.method,
                msg.url,
                headers=msg.headers,
                params=msg.params,
                json=msg.body,
            )
            limited, transient = detect_limit(self._rules, response)
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            error_text = f"{type(exc).__name__}: {exc}"
            logger.warning("api=%s msg=%s downstream error: %s", api, msg.id, error_text)

        # 1) 限流：立刻降速
        if limited:
            decision = await self._pacer.report_api_limit(api)
            self._count("limited", api)
            error_text = error_text or f"rate limited (http {response.status_code if response else 'n/a'})"
            logger.warning(
                "api=%s rate limited -> api_rate=%.2f global_rate=%.2f distinct=%d global_cut=%s",
                api,
                decision.api_rate,
                decision.global_rate,
                decision.distinct_apis,
                decision.global_decreased,
            )
            await self._retry_or_dead(api, msg, error_text, response)
            return

        # 2) 成功
        if response is not None and not transient and response.status_code < 400:
            await self._pacer.report_api_success(api)
            self._count("succeeded", api)
            body_preview = response.text[:4000]
            await self._queue.write_status(
                msg, "succeeded", None, response.status_code, body_preview
            )
            logger.info("api=%s msg=%s succeeded attempt=%d", api, msg.id, msg.attempt)
            return

        # 3) 临时失败：超时/连接错误、繁忙返回体、5xx
        if response is not None and error_text is None:
            error_text = f"transient failure (http {response.status_code}): {response.text[:500]}"
        logger.info("api=%s msg=%s transient: %s", api, msg.id, error_text)
        await self._retry_or_dead(api, msg, error_text, response)

    async def _retry_or_dead(
        self,
        api: str,
        msg: Message,
        error_text: str,
        response: httpx.Response | None,
    ) -> None:
        status = response.status_code if response is not None else None
        body = response.text[:4000] if response is not None else None

        if msg.attempt > msg.max_retries:
            reason = f"max retries ({msg.max_retries}) exceeded; last error: {error_text}"
            await self._queue.send_to_dlq(msg, reason, status, body)
            self._count("dead", api)
            logger.error("api=%s msg=%s moved to DLQ: %s", api, msg.id, reason)
            return

        delay = self._backoff_delay(msg.attempt)
        due = time.time() + delay
        await self._queue.delay_for_retry(msg, due)
        await self._queue.write_status(msg, "retrying", error_text, status, body)
        self._count("retried", api)
        logger.info(
            "api=%s msg=%s retry %d/%d in %.2fs",
            api,
            msg.id,
            msg.attempt,
            msg.max_retries,
            delay,
        )

    def _backoff_delay(self, attempt: int) -> float:
        base = min(
            self._s.backoff_max,
            self._s.backoff_base * (self._s.backoff_factor ** (attempt - 1)),
        )
        jitter = base * self._s.backoff_jitter * (2 * random.random() - 1)
        # 抖动后再夹一次，保证最终延迟不超过配置上限
        return max(0.0, min(self._s.backoff_max, base + jitter))

    # ---------- 计数与刷库 ----------

    def _count(self, field: str, api: str) -> None:
        bucket = now_bucket()
        for scope in ("global", api):
            counters = self._counters.setdefault((bucket, scope), self._new_counter())
            counters[field] += 1

    @staticmethod
    def _new_counter() -> dict[str, int]:
        return {
            "requested": 0,
            "succeeded": 0,
            "limited": 0,
            "retried": 0,
            "dead": 0,
        }

    async def _flush_counters(self) -> None:
        if not self._counters:
            return
        pending = self._counters
        self._counters = {}
        for (bucket, scope), values in pending.items():
            await self._stats.increment(
                bucket,
                scope,
                requested=values["requested"],
                succeeded=values["succeeded"],
                limited=values["limited"],
                retried=values["retried"],
                dead=values["dead"],
            )
