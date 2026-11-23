"""自适应均速器测试。"""

from __future__ import annotations

import pytest

from qwsmoothq.config import Settings
from qwsmoothq.pacer import AdaptivePacer


@pytest.mark.asyncio
async def test_initial_acquire_allowed_then_smoothed(pacer, settings):
    # 桶初始有 1 个令牌：第一次放行，紧接着第二次应被拒绝（匀速再生）
    assert await pacer.acquire("api-a", now=100.0) is True
    assert await pacer.acquire("api-a", now=100.0) is False
    # 被拒不推进时钟：半周期（0.05s）只补到 0.5 个令牌，继续拒绝
    assert await pacer.acquire("api-a", now=100.05) is False
    # rate=10 => 完整补票周期 0.1s，100.11 时补满，再次放行
    assert await pacer.acquire("api-a", now=100.11) is True
    # 距上次成功仅 0.05s，还不够一个周期
    assert await pacer.acquire("api-a", now=100.16) is False
    # 距上次成功满 0.1s，再次放行
    assert await pacer.acquire("api-a", now=100.22) is True


@pytest.mark.asyncio
async def test_global_bucket_also_gates(pacer, settings):
    # 两个不同接口共享全局桶；全局桶初始也只有 1 个令牌
    assert await pacer.acquire("api-a", now=200.0) is True
    assert await pacer.acquire("api-b", now=200.0) is False
    # 等全局桶补满一个票（global rate=10 => 约 0.1s，取 0.11 留余量）
    assert await pacer.acquire("api-b", now=200.11) is True


@pytest.mark.asyncio
async def test_limit_cuts_api_rate(pacer, settings):
    before = await pacer.get_rate("api:api-a")
    decision = await pacer.report_api_limit("api-a", now=300.0)
    assert decision.api_rate == pytest.approx(before * (1 - settings.rate_decrease_pct))
    assert decision.global_decreased is False  # 单接口限流，暂不动全局
    assert decision.distinct_apis == 1
    assert await pacer.get_rate("api:api-a") == decision.api_rate


@pytest.mark.asyncio
async def test_multiple_apis_limit_cuts_global(pacer, settings):
    global_before = await pacer.get_rate("global")
    d1 = await pacer.report_api_limit("api-a", now=400.0)
    assert d1.global_decreased is False
    d2 = await pacer.report_api_limit("api-b", now=401.0)
    assert d2.global_decreased is True
    assert d2.distinct_apis == 2
    assert d2.global_rate == pytest.approx(
        global_before * (1 - settings.rate_decrease_pct)
    )


@pytest.mark.asyncio
async def test_old_limit_events_expire(pacer, settings):
    # 超过关联窗口的旧事件不参与联动判定
    await pacer.report_api_limit("api-a", now=500.0)
    much_later = 500.0 + settings.limit_correlation_window + 1
    d2 = await pacer.report_api_limit("api-b", now=much_later)
    assert d2.distinct_apis == 1
    assert d2.global_decreased is False


@pytest.mark.asyncio
async def test_rate_never_below_min(pacer, settings):
    for i in range(20):
        decision = await pacer.report_api_limit("api-a", now=600.0 + i)
    assert decision.api_rate >= settings.min_rate


@pytest.mark.asyncio
async def test_backlog_tick_increases_rate(pacer, settings):
    # 无撞限历史，积压超阈值 => 提速，且不超过 max_rate
    before = await pacer.get_rate("api:api-a")
    rate = await pacer.tick("api:api-a", backlog=settings.backlog_threshold + 10,
                            max_rate=settings.max_rate, now=700.0)
    assert rate == pytest.approx(before * (1 + settings.rate_increase_pct))

    for i in range(30):
        rate = await pacer.tick(
            "api:api-a", backlog=100, max_rate=settings.max_rate, now=701.0 + i
        )
    assert rate <= settings.max_rate


@pytest.mark.asyncio
async def test_tick_no_increase_when_backlog_low(pacer, settings):
    before = await pacer.get_rate("api:api-a")
    rate = await pacer.tick("api:api-a", backlog=0,
                            max_rate=settings.max_rate, now=800.0)
    assert rate == pytest.approx(before)


@pytest.mark.asyncio
async def test_cooldown_blocks_backlog_ramp(pacer, settings):
    await pacer.report_api_limit("api-a", now=900.0)
    rate_after_limit = await pacer.get_rate("api:api-a")
    # 冷却窗口内即便积压很大也不提速
    rate = await pacer.tick(
        "api:api-a", backlog=999, max_rate=settings.max_rate,
        now=900.0 + settings.limit_correlation_window - 1,
    )
    assert rate == pytest.approx(rate_after_limit)


@pytest.mark.asyncio
async def test_success_streak_slow_recovery(pacer, settings):
    await pacer.report_api_limit("api-a", now=1000.0)
    cut_rate = await pacer.get_rate("api:api-a")
    after_cooldown = 1000.0 + settings.limit_correlation_window + 1

    # 连续成功但未达阈值：不回升
    for _ in range(settings.success_streak_needed - 1):
        rate = await pacer.report_success("api:api-a", now=after_cooldown)
    assert rate == pytest.approx(cut_rate)

    # 达到阈值：按 recovery_increase_pct 缓慢回升
    rate = await pacer.report_success("api:api-a", now=after_cooldown + 1)
    assert rate == pytest.approx(cut_rate * (1 + settings.recovery_increase_pct))


@pytest.mark.asyncio
async def test_success_recovery_blocked_in_cooldown(pacer, settings):
    await pacer.report_api_limit("api-a", now=1100.0)
    cut_rate = await pacer.get_rate("api:api-a")
    for _ in range(settings.success_streak_needed + 2):
        rate = await pacer.report_success("api:api-a", now=1100.0 + 1)
    assert rate == pytest.approx(cut_rate)


@pytest.mark.asyncio
async def test_report_api_success_touches_both_scopes(pacer, settings):
    # 不抛异常且全局 streak 也被推进即视为双层都记账
    await pacer.report_api_success("api-a", now=1200.0)
    rates = await pacer.get_all_rates(["api-a"])
    assert "global" in rates and "api:api-a" in rates


@pytest.mark.asyncio
async def test_rate_state_persists_across_pacer_instances(redis_client, settings):
    pacer1 = AdaptivePacer(redis_client, settings)
    await pacer1.reset()
    await pacer1.report_api_limit("api-a", now=1300.0)
    stored = await pacer1.get_rate("api:api-a")

    # 模拟进程重启：新实例读取同一 Redis 状态
    pacer2 = AdaptivePacer(redis_client, settings)
    assert await pacer2.get_rate("api:api-a") == pytest.approx(stored)


def test_settings_custom_values():
    s = Settings(backlog_threshold=99, max_rate=42.0)
    assert s.backlog_threshold == 99
    assert s.max_rate == 42.0
