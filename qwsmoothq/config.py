"""全局配置。所有配置项均可通过 QWSQ_ 前缀的环境变量覆盖（见 .env.example）。"""

from __future__ import annotations

import json
from typing import Any

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="QWSQ_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # 基础设施
    redis_url: str = "redis://localhost:6379/0"
    redis_key_prefix: str = "qwsmoothq"
    database_url: str = ""  # 为空时降级为内存统计
    http_timeout: float = 10.0

    # 速率上下限（请求/秒）
    initial_rate: float = 5.0
    min_rate: float = 0.5
    max_rate: float = 20.0
    global_max_rate: float = 50.0

    # 自适应调速
    backlog_threshold: int = 20
    rate_increase_pct: float = 0.2
    rate_decrease_pct: float = 0.5
    recovery_increase_pct: float = 0.05
    success_streak_needed: int = 10
    # 多少秒内、多少个不同接口触发限流 => 联动降全局
    limit_correlation_window: int = 10
    limit_correlation_min_apis: int = 2

    # 退避重试
    backoff_base: float = 0.5
    backoff_factor: float = 2.0
    backoff_max: float = 30.0
    backoff_jitter: float = 0.25

    # 限流规则
    limit_rules: list[str] = Field(default_factory=lambda: ["http429", "wecom"])
    wecom_limit_errcodes: list[int] = Field(default_factory=lambda: [45009, 45011])
    wecom_busy_errcodes: list[int] = Field(default_factory=lambda: [-1])

    # 消息状态保留
    status_ttl_hours: int = 72

    @field_validator("limit_rules", "wecom_limit_errcodes", "wecom_busy_errcodes", mode="before")
    @classmethod
    def _parse_json_list(cls, value: Any) -> Any:
        """环境变量里的 JSON 数组字符串（如 '["http429"]'）解析成 list。"""
        if isinstance(value, str):
            value = value.strip()
            if value.startswith("["):
                return json.loads(value)
            return [item.strip() for item in value.split(",") if item.strip()]
        return value


def get_settings() -> Settings:
    return Settings()
