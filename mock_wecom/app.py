"""本地 Mock 服务：模拟企业微信风格接口与可注入的限流行为。

模拟企微惯例：HTTP 始终 200，错误通过 JSON ``errcode`` 表达。
令牌桶内存维护，进程重启即复位。

- 单接口限流：POST /mock/control/limit  body {"api": "message/send", "rps": 2}
  之后该接口超过 rps 的请求返回 errcode=45009
- 全局限流：body {"api": "*", "rps": 5}，所有接口共享一个全局限流桶
- 解除：body {"api": "...", "rps": null}
- 系统繁忙（可重试、非限流）：body {"api": "...", "busy_rate": 0.5}
- 查看当前配置：GET /mock/control
"""

from __future__ import annotations

import time
from typing import Any

from fastapi import FastAPI
from pydantic import BaseModel

from .limiter import TokenBucket

app = FastAPI(title="mock-wecom", version="0.1.0")


class LimitControl(BaseModel):
    api: str
    rps: float | None = None
    busy_rate: float | None = None


# api 标识 -> 限流桶；"*" 为全局限流桶
_limiters: dict[str, TokenBucket] = {}
# api 标识 -> 繁忙概率
_busy_rates: dict[str, float] = {}
_request_log: list[dict[str, Any]] = []


def _api_of(path: str) -> str:
    # /cgi-bin/message/send -> message/send
    return path.removeprefix("/cgi-bin/").strip("/") or "root"


def _ok(payload: dict[str, Any]) -> dict[str, Any]:
    return {"errcode": 0, "errmsg": "ok", **payload}


@app.post("/mock/control/limit")
async def set_limit(ctrl: LimitControl) -> dict:
    if ctrl.rps is None:
        _limiters.pop(ctrl.api, None)
    else:
        _limiters[ctrl.api] = TokenBucket(rate=ctrl.rps, capacity=1.0)
    if ctrl.busy_rate is None:
        _busy_rates.pop(ctrl.api, None)
    else:
        _busy_rates[ctrl.api] = ctrl.busy_rate
    return {"ok": True, "limits": _snapshot()}


@app.get("/mock/control")
async def get_control() -> dict:
    return {"limits": _snapshot(), "busy_rates": dict(_busy_rates)}


@app.get("/mock/requests")
async def get_requests(limit: int = 100) -> dict:
    return {"requests": _request_log[-limit:], "total": len(_request_log)}


@app.post("/mock/reset")
async def reset() -> dict:
    _limiters.clear()
    _busy_rates.clear()
    _request_log.clear()
    return {"ok": True}


def _snapshot() -> dict[str, float]:
    return {api: bucket.rate for api, bucket in _limiters.items()}


def _check_limited(api: str) -> bool:
    # 先检查全局限流桶，再检查单接口桶；任一拒绝即限流
    for scope in ("*", api):
        bucket = _limiters.get(scope)
        if bucket is not None and not bucket.try_acquire(time.time()):
            return True
    return False


@app.api_route("/cgi-bin/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
async def cgi_bin_proxy(path: str) -> dict:
    api = _api_of(f"/cgi-bin/{path}")
    limited = _check_limited(api)
    entry = {"ts": time.time(), "api": api, "limited": limited}
    _request_log.append(entry)

    if limited:
        # 企微限流错误码：45009 接口调用超过限制
        return {"errcode": 45009, "errmsg": "api freq out of limit (mock)"}

    busy_rate = _busy_rates.get(api, 0.0)
    if busy_rate > 0 and (time.time_ns() % 1000) / 1000.0 < busy_rate:
        # 企微系统繁忙：可重试但不算限流
        return {"errcode": -1, "errmsg": "system busy (mock)"}

    return _ok({"api": api, "echo_at": entry["ts"]})
