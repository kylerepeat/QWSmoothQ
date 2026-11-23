"""可插拔的限流信号判定规则。

内置规则：
- ``http429``：HTTP 状态码 429
- ``wecom``  ：企业微信风格——HTTP 200 但 JSON 返回体 errcode 命中限流码
  （默认 45009 接口调用超过限制、45011 接口速度限制，具体以企微开发者文档为准）；
  errcode 命中"繁忙"码（默认 -1）时不算限流，但判定为可重试的临时失败。

自定义规则：实现 :class:`LimitRule` 接口，并在配置 ``QWSQ_LIMIT_RULES`` 中填入
``"模块路径:类名"`` 即可插入，例如 ``"myapp.rules:MyRule"``（类需支持无参构造，
或同时是接收 settings 的单例工厂）。
"""

from __future__ import annotations

import importlib
from typing import Protocol, runtime_checkable

import httpx

from .config import Settings


@runtime_checkable
class LimitRule(Protocol):
    """限流判定规则接口。"""

    def is_rate_limited(self, response: httpx.Response) -> bool: ...

    def is_transient(self, response: httpx.Response) -> bool:
        """非限流但值得重试的临时失败（如企微系统繁忙）。默认否。"""
        ...


class Http429Rule:
    """最通用的限流信号：HTTP 429。"""

    def is_rate_limited(self, response: httpx.Response) -> bool:
        return response.status_code == 429

    def is_transient(self, response: httpx.Response) -> bool:
        return False


class WeComRule:
    """企业微信返回体规则。

    企微接口通常 HTTP 200，通过 JSON 中的 ``errcode`` 表达错误：
    - 限流码（默认 45009 / 45011）=> 限流，立即降频
    - 繁忙码（默认 -1 系统繁忙）=> 非限流但可重试
    """

    def __init__(
        self,
        limit_errcodes: tuple[int, ...] = (45009, 45011),
        busy_errcodes: tuple[int, ...] = (-1,),
    ) -> None:
        self.limit_errcodes = set(limit_errcodes)
        self.busy_errcodes = set(busy_errcodes)

    def _errcode(self, response: httpx.Response) -> int | None:
        try:
            data = response.json()
        except (ValueError, TypeError):
            return None
        if not isinstance(data, dict) or "errcode" not in data:
            return None
        try:
            return int(data["errcode"])
        except (ValueError, TypeError):
            return None

    def is_rate_limited(self, response: httpx.Response) -> bool:
        errcode = self._errcode(response)
        return errcode is not None and errcode in self.limit_errcodes

    def is_transient(self, response: httpx.Response) -> bool:
        errcode = self._errcode(response)
        return errcode is not None and errcode in self.busy_errcodes


def build_rules(settings: Settings) -> list[LimitRule]:
    """按配置实例化规则链。内置名 -> 类；其余按 ``module:Class`` 动态导入。"""
    builtin: dict[str, LimitRule] = {
        "http429": Http429Rule(),
        "wecom": WeComRule(
            limit_errcodes=tuple(settings.wecom_limit_errcodes),
            busy_errcodes=tuple(settings.wecom_busy_errcodes),
        ),
    }
    rules: list[LimitRule] = []
    for name in settings.limit_rules:
        key = name.strip()
        if key in builtin:
            rules.append(builtin[key])
            continue
        if ":" not in key:
            raise ValueError(
                f"未知限流规则 {key!r}：内置规则为 {sorted(builtin)}，"
                f"自定义规则请使用 '模块路径:类名' 形式"
            )
        module_path, class_name = key.split(":", 1)
        module = importlib.import_module(module_path)
        candidate = getattr(module, class_name)
        # 优先无参构造；类显式接收 settings 时再传入（自定义规则工厂）
        try:
            rule = candidate()
        except TypeError:
            rule = candidate(settings)
        if not hasattr(rule, "is_rate_limited"):
            raise TypeError(f"自定义规则 {key} 未实现 is_rate_limited()")
        rules.append(rule)
    if not rules:
        raise ValueError("至少需要配置一条限流判定规则")
    return rules


def detect_limit(rules: list[LimitRule], response: httpx.Response) -> tuple[bool, bool]:
    """返回 (是否限流, 是否可重试临时失败)。任一规则命中即成立。"""
    limited = any(rule.is_rate_limited(response) for rule in rules)
    transient = any(rule.is_transient(response) for rule in rules)
    return limited, transient
