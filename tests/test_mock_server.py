"""Mock 企微服务测试：限流注入、繁忙返回与控制接口。"""

from __future__ import annotations

import httpx
import pytest

from mock_wecom.app import app
from mock_wecom.limiter import TokenBucket


@pytest.mark.asyncio
async def test_mock_default_success():
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://mock"
    ) as client:
        resp = await client.post("/cgi-bin/message/send", json={"a": 1})
        assert resp.status_code == 200
        assert resp.json()["errcode"] == 0


@pytest.mark.asyncio
async def test_mock_per_api_limit():
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://mock"
    ) as client:
        await client.post("/mock/reset")
        # 单接口 2 rps：桶容量 1，首请求放行，紧接着的第二请求被限
        await client.post("/mock/control/limit", json={"api": "message/send", "rps": 2})
        first = await client.post("/cgi-bin/message/send")
        second = await client.post("/cgi-bin/message/send")
        assert first.json()["errcode"] == 0
        assert second.json()["errcode"] == 45009

        # 其他接口不受影响
        other = await client.post("/cgi-bin/user/get")
        assert other.json()["errcode"] == 0


@pytest.mark.asyncio
async def test_mock_global_limit():
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://mock"
    ) as client:
        await client.post("/mock/reset")
        await client.post("/mock/control/limit", json={"api": "*", "rps": 5})
        first = await client.post("/cgi-bin/any/api")
        second = await client.post("/cgi-bin/other/api")
        assert first.json()["errcode"] == 0
        assert second.json()["errcode"] == 45009  # 全局桶拦下第二个接口


@pytest.mark.asyncio
async def test_mock_control_snapshot_and_remove_limit():
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://mock"
    ) as client:
        await client.post("/mock/reset")
        await client.post("/mock/control/limit", json={"api": "message/send", "rps": 3})
        snapshot = (await client.get("/mock/control")).json()
        assert snapshot["limits"]["message/send"] == 3

        await client.post("/mock/control/limit", json={"api": "message/send", "rps": None})
        snapshot = (await client.get("/mock/control")).json()
        assert "message/send" not in snapshot["limits"]


def test_token_bucket_refill():
    bucket = TokenBucket(rate=10.0, capacity=1.0)
    assert bucket.try_acquire(now=0.0) is True
    assert bucket.try_acquire(now=0.0) is False
    # 被拒不推进时钟：0.05s 时只补到 0.5 个令牌，仍被拒
    assert bucket.try_acquire(now=0.05) is False
    # 完整补票周期（1/rate = 0.1s）后补满 1 个令牌，放行
    assert bucket.try_acquire(now=0.11) is True
    # 连续两次成功之间至少间隔一个补票周期：0.16s 距上次成功仅 0.05s
    assert bucket.try_acquire(now=0.16) is False
    # 再等到距上次成功满 0.1s（0.22s），再次放行
    assert bucket.try_acquire(now=0.22) is True
