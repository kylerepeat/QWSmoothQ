"""Redis 队列测试：FIFO、延迟搬运、积压计数、死信与状态查询。"""

from __future__ import annotations

import pytest

from qwsmoothq.models import EnqueueRequest, Message


def _make_msg(api: str = "message/send", idx: int = 0, max_retries: int = 3) -> Message:
    req = EnqueueRequest(
        api=api,
        url=f"http://downstream.example/cgi-bin/{api}",
        body={"index": idx},
        max_retries=max_retries,
    )
    return Message.from_request(req)


@pytest.mark.asyncio
async def test_enqueue_fifo_and_backlog(queue):
    for i in range(3):
        await queue.enqueue(_make_msg(idx=i))

    assert await queue.total_backlog() == 3
    seen = []
    for _ in range(3):
        api, msg = await queue.pop_ready_round_robin()
        assert api == "message/send"
        seen.append(msg.body["index"])
    assert seen == [0, 1, 2]  # LPUSH + RPOP => FIFO
    assert await queue.total_backlog() == 0


@pytest.mark.asyncio
async def test_round_robin_across_apis(queue):
    await queue.enqueue(_make_msg(api="api-a", idx=1))
    await queue.enqueue(_make_msg(api="api-b", idx=2))
    await queue.enqueue(_make_msg(api="api-a", idx=3))

    popped_apis = []
    for _ in range(3):
        api, _ = await queue.pop_ready_round_robin()
        popped_apis.append(api)
    assert popped_apis == ["api-a", "api-b", "api-a"]


@pytest.mark.asyncio
async def test_delayed_retry_move_due(queue):
    msg = _make_msg()
    await queue.enqueue(msg)
    api, popped = await queue.pop_ready_round_robin()
    assert popped is not None
    assert await queue.total_backlog() == 0

    # 放到 10 秒后
    await queue.delay_for_retry(popped, 100.0)
    assert (await queue.move_due(now=50.0)) == 0
    assert await queue.total_backlog() == 0

    # 到期后搬回就绪队列
    assert (await queue.move_due(now=101.0)) == 1
    assert await queue.total_backlog() == 1
    api2, popped2 = await queue.pop_ready_round_robin()
    assert popped2.id == msg.id
    assert api2 == "message/send"


@pytest.mark.asyncio
async def test_return_head_restores_backlog(queue):
    msg = _make_msg()
    await queue.enqueue(msg)
    api, popped = await queue.pop_ready_round_robin()
    assert await queue.backlog(api) == 0
    await queue.return_head(api, popped)
    assert await queue.backlog(api) == 1


@pytest.mark.asyncio
async def test_dlq_lifecycle_and_requeue(queue):
    msg = _make_msg(max_retries=1)
    await queue.enqueue(msg)

    dl = await queue.send_to_dlq(msg, "boom", 429, '{"errcode":45009}')
    assert dl.reason == "boom"
    assert dl.response_status == 429

    dlq = await queue.list_dlq()
    assert len(dlq) == 1 and dlq[0].message.id == msg.id

    # 死信标记防止重复入队
    await queue.send_to_dlq(msg, "again", None, None)
    assert len(await queue.list_dlq()) == 1

    status = await queue.get_status(msg.id)
    assert status is not None and status.state == "dead"
    assert status.last_error == "boom"

    requeued = await queue.requeue_dlq(msg.id)
    assert requeued is not None and requeued.attempt == 0
    assert len(await queue.list_dlq()) == 0
    status2 = await queue.get_status(msg.id)
    assert status2.state == "queued"
    assert await queue.total_backlog() == 1


@pytest.mark.asyncio
async def test_status_ttl_written(queue):
    msg = _make_msg()
    await queue.enqueue(msg)
    status = await queue.get_status(msg.id)
    assert status is not None
    assert status.state == "queued"
    assert status.attempt == 0
    assert await queue.get_status("missing-id") is None


@pytest.mark.asyncio
async def test_backlogs_per_api(queue):
    await queue.enqueue(_make_msg(api="a"))
    await queue.enqueue(_make_msg(api="a"))
    await queue.enqueue(_make_msg(api="b"))
    assert await queue.backlogs() == {"a": 2, "b": 1}
