"""限流规则测试：HTTP 429 与企微 errcode 规则、规则链装配。"""

from __future__ import annotations

import httpx
import pytest

from qwsmoothq.config import Settings
from qwsmoothq.rules import Http429Rule, WeComRule, build_rules, detect_limit


def _resp(status_code: int = 200, body: dict | None = None) -> httpx.Response:
    return httpx.Response(status_code, json=body if body is not None else {})


def test_http429_rule():
    rule = Http429Rule()
    assert rule.is_rate_limited(_resp(429))
    assert not rule.is_rate_limited(_resp(200))
    assert not rule.is_transient(_resp(429))


def test_wecom_limit_errcodes():
    rule = WeComRule()
    assert rule.is_rate_limited(_resp(200, {"errcode": 45009, "errmsg": "freq out of limit"}))
    assert rule.is_rate_limited(_resp(200, {"errcode": 45011, "errmsg": "speed limit"}))
    assert not rule.is_rate_limited(_resp(200, {"errcode": 0}))


def test_wecom_busy_is_transient_but_not_limited():
    rule = WeComRule()
    resp = _resp(200, {"errcode": -1, "errmsg": "system busy"})
    assert not rule.is_rate_limited(resp)
    assert rule.is_transient(resp)


def test_wecom_non_json_body_safe():
    rule = WeComRule()
    resp = httpx.Response(502, content=b"bad gateway", headers={"content-type": "text/plain"})
    assert not rule.is_rate_limited(resp)
    assert not rule.is_transient(resp)


def test_detect_limit_chain():
    settings = Settings()
    rules = build_rules(settings)
    limited, transient = detect_limit(rules, _resp(429))
    assert limited and not transient

    limited, transient = detect_limit(rules, _resp(200, {"errcode": 45009}))
    assert limited and not transient

    limited, transient = detect_limit(rules, _resp(200, {"errcode": -1}))
    assert not limited and transient

    limited, transient = detect_limit(rules, _resp(200, {"errcode": 0}))
    assert not limited and not transient


def test_build_rules_unknown_name_raises():
    settings = Settings(limit_rules=["does-not-exist"])
    with pytest.raises(ValueError, match="未知限流规则"):
        build_rules(settings)


def test_build_rules_empty_raises():
    settings = Settings(limit_rules=[])
    with pytest.raises(ValueError, match="至少需要配置一条"):
        build_rules(settings)


def test_custom_rule_via_import_path():
    settings = Settings(limit_rules=["qwsmoothq.rules:Http429Rule"])
    rules = build_rules(settings)
    assert len(rules) == 1
    assert isinstance(rules[0], Http429Rule)
