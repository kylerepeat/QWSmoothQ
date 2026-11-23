"""消费者端到端测试（fakeredis + 脚本化 httpx transport，无外部依赖）。"""

from __future__ import annotations

import asyncio

import httpx
import pytest

from qwsmoothq.consumer import Consumer
from qwsmoothq.models import EnqueueRequest, Message
from qwsmoothq.rules import build_rules
from qwsmoothq.stats import now_bucket


def _build(settings, queue, pacer, stats, transport) -> Consumer:
    return Consumer(
        queue=queue,
        pacer=pacer,
        stats=stats,
        rules=build_rules(settings),
        settings=settings,
        transport=transport,
    )


def _msg(api: str = "message/send", max_retries: int = 3) -> Message:
    return Message.from_request(
        EnqueueRequest(
            api=api,
            url=f"http://downstream.example/cgi-bin/{api}",
            body={"hello": api},
            max_retries=max_retries,
        )
    )


async def _run_until(condition, timeout: float = 5.0, interval: float = 0.02) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if await condition():
            return
        await asyncio.sleep(interval)
    raise AssertionError("condition not met before timeout")


async def _state_is(queue, message_id, state):
    status = await queue.get_status(message_id)
    return status is not None and status.state == state


async def _both_succeeded(queue, id_a, id_b):
    sa = await queue.get_status(id_a)
    sb = await queue.get_status(id_b)
    return (sa and sa.state == "succeeded" and sb and sb.state == "succeeded")


@pytest.mark.asyncio
async def test_success_path(queue, pacer, stats, settings, scripted_transport):
    consumer = _build(settings, queue, pacer, stats, scripted_transport)
    msg = _msg()
    await queue.enqueue(msg)
    await consumer.start()
    try:
        await _run_until(lambda: _state_is(queue, msg.id, "succeeded"))
    finally:
        await consumer.stop()

    status = await queue.get_status(msg.id)
    assert status.state == "succeeded"
    assert status.response_status == 200
    assert len(scripted_transport.calls) == 1


@pytest.mark.asyncio
async def test_limit_then_retry_success(queue, pacer, stats, settings, scripted_transport):
    # 前两次撞企微限流，第三次成功
    scripted_transport.push_response(200, {"errcode": 45009, "errmsg": "limited"})
    scripted_transport.push_response(200, {"errcode": 45009, "errmsg": "limited"})
    scripted_transport.push_response(200, {"errcode": 0, "errmsg": "ok"})

    consumer = _build(settings, queue, pacer, stats, scripted_transport)
    rate_before = await pacer.get_rate("api:message/send")
    msg = _msg(max_retries=5)
    await queue.enqueue(msg)
    await consumer.start()
    try:
        await _run_until(lambda: _state_is(queue, msg.id, "succeeded"))
    finally:
        await consumer.stop()

    status = await queue.get_status(msg.id)
    assert status.state == "succeeded"
    assert status.attempt == 3
    rate_after = await pacer.get_rate("api:message/send")
    assert rate_after < rate_before  # 撞限流后接口速率被下调
    assert len(scripted_transport.calls) == 3


@pytest.mark.asyncio
async def test_retries_exhausted_goes_dlq(queue, pacer, stats, settings, scripted_transport):
    for _ in range(4):
        scripted_transport.push_response(200, {"errcode": 45009})

    consumer = _build(settings, queue, pacer, stats, scripted_transport)
    msg = _msg(max_retries=2)  # 共尝试 3 次（首次 + 2 次重试）
    await queue.enqueue(msg)
    await consumer.start()
    try:
        await _run_until(lambda: _state_is(queue, msg.id, "dead"))
    finally:
        await consumer.stop()

    dlq = await queue.list_dlq()
    assert len(dlq) == 1
    assert dlq[0].message.id == msg.id
    assert "max retries" in dlq[0].reason
    assert dlq[0].response_body is not None and "45009" in dlq[0].response_body
    assert len(scripted_transport.calls) == 3


@pytest.mark.asyncio
async def test_timeout_is_retried_without_rate_cut(queue, pacer, stats, settings,
                                                    scripted_transport):
    rate_before = await pacer.get_rate("api:message/send")
    scripted_transport.push_exception(httpx.ConnectTimeout("timed out"))
    scripted_transport.push_response(200, {"errcode": 0})

    consumer = _build(settings, queue, pacer, stats, scripted_transport)
    msg = _msg()
    await queue.enqueue(msg)
    await consumer.start()
    try:
        await _run_until(lambda: _state_is(queue, msg.id, "succeeded"))
    finally:
        await consumer.stop()

    status = await queue.get_status(msg.id)
    assert status.state == "succeeded"
    assert status.attempt == 2
    # 超时不触发降速
    assert await pacer.get_rate("api:message/send") == rate_before


@pytest.mark.asyncio
async def test_busy_errcode_is_transient_retry(queue, pacer, stats, settings,
                                                scripted_transport):
    scripted_transport.push_response(200, {"errcode": -1, "errmsg": "busy"})
    scripted_transport.push_response(200, {"errcode": 0})

    consumer = _build(settings, queue, pacer, stats, scripted_transport)
    msg = _msg()
    await queue.enqueue(msg)
    await consumer.start()
    try:
        await _run_until(lambda: _state_is(queue, msg.id, "succeeded"))
    finally:
        await consumer.stop()
    assert (await queue.get_status(msg.id)).attempt == 2


@pytest.mark.asyncio
async def test_backoff_has_jitter_bounds(settings):
    consumer = Consumer(
        queue=None, pacer=None, stats=None, rules=[], settings=settings,  # type: ignore[arg-type]
    )
    delays = [consumer._backoff_delay(1) for _ in range(50)]
    base = settings.backoff_base
    assert all(base * (1 - settings.backoff_jitter) <= d <= base * (1 + settings.backoff_jitter)
               for d in delays)
    assert min(delays) < max(delays)  # 抖动确实存在
    # 退避递增且受上限约束
    assert consumer._backoff_delay(10) <= settings.backoff_max


@pytest.mark.asyncio
async def test_backlog_drives_rate_ramp(queue, pacer, stats, settings, scripted_transport):
    # 下游一律慢到让消息积压；直接验证 tick 提速逻辑
    for i in range(10):
        await queue.enqueue(_msg(api="ramp-api", max_retries=0))

    rate_before = await pacer.get_rate("api:ramp-api")
    consumer = _build(settings, queue, pacer, stats, scripted_transport)
    await consumer.run_tick_once()
    rate_after = await pacer.get_rate("api:ramp-api")
    assert rate_after > rate_before


@pytest.mark.asyncio
async def test_multi_api_limit_correlation(queue, pacer, stats, settings, scripted_transport):
    # 两个接口同一窗口内各撞一次限流 => 全局速率联动下调
    global_before = await pacer.get_rate("global")
    scripted_transport.push_response(200, {"errcode": 45009})
    scripted_transport.push_response(200, {"errcode": 45009})
    for _ in range(2):  # 随后成功，避免进 DLQ
        scripted_transport.push_response(200, {"errcode": 0})

    consumer = _build(settings, queue, pacer, stats, scripted_transport)
    msg_a = _msg(api="api-a", max_retries=5)
    msg_b = _msg(api="api-b", max_retries=5)
    await queue.enqueue(msg_a)
    await queue.enqueue(msg_b)
    await consumer.start()
    try:
        await _run_until(lambda: _both_succeeded(queue, msg_a.id, msg_b.id))
    finally:
        await consumer.stop()

    assert await pacer.get_rate("global") < global_before


async def _both_succeeded(queue, id_a, id_b):
    sa = await queue.get_status(id_a)
    sb = await queue.get_status(id_b)
    return (sa and sa.state == "succeeded" and sb and sb.state == "succeeded")


@pytest.mark.asyncio
async def test_stats_recorded_per_second(queue, pacer, stats, settings, scripted_transport):
    consumer = _build(settings, queue, pacer, stats, scripted_transport)
    for _ in range(3):
        await queue.enqueue(_msg())
    await consumer.start()
    try:
        await asyncio.sleep(0.5)
        await consumer.run_tick_once()  # 强制把当前秒计数刷入统计
    finally:
        await consumer.stop()

    bucket = now_bucket()
    points = await stats.query("global", bucket - 1, bucket + 1)
    assert points, "全局统计点应已落库"
    total = sum(point.requested for point in points)
    assert total == 3
    assert sum(point.succeeded for point in points) == 3

    api_points = await stats.query("message/send", bucket - 1, bucket + 1)
    assert sum(point.requested for point in api_points) == 3
    # 采样点带积压与速率
    assert any(point.rate is not None for point in api_points)
