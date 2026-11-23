"""按秒统计：抽象接口、PostgreSQL 实现与内存降级实现。

每个统计点对应 (秒级时间桶, 作用域)，作用域为 ``global`` 或具体接口标识。
未配置 ``QWSQ_DATABASE_URL`` 时使用内存实现（进程重启即丢失），方便本地试玩与测试。
"""

from __future__ import annotations

import time
from typing import Protocol

from .models import StatsPoint

_STATS_COLUMNS = ("requested", "succeeded", "limited", "retried", "dead", "backlog", "rate")


class StatsSink(Protocol):
    async def init(self) -> None: ...

    async def increment(
        self,
        bucket: int,
        scope: str,
        *,
        requested: int = 0,
        succeeded: int = 0,
        limited: int = 0,
        retried: int = 0,
        dead: int = 0,
    ) -> None: ...

    async def record_backlog_and_rate(
        self, bucket: int, scope: str, backlog: int, rate: float | None
    ) -> None: ...

    async def query(
        self, scope: str, start_bucket: int, end_bucket: int
    ) -> list[StatsPoint]: ...

    async def close(self) -> None: ...


def _empty_point(bucket: int, scope: str) -> StatsPoint:
    return StatsPoint(bucket=bucket, scope=scope)


class MemoryStats:
    """进程内存统计。键为 (bucket, scope)，自动清理 1 小时前的旧桶。"""

    def __init__(self, retention_seconds: int = 3600) -> None:
        self._points: dict[tuple[int, str], StatsPoint] = {}
        self._retention = retention_seconds

    async def init(self) -> None:
        return None

    async def increment(
        self,
        bucket: int,
        scope: str,
        *,
        requested: int = 0,
        succeeded: int = 0,
        limited: int = 0,
        retried: int = 0,
        dead: int = 0,
    ) -> None:
        point = self._points.setdefault((bucket, scope), _empty_point(bucket, scope))
        point.requested += requested
        point.succeeded += succeeded
        point.limited += limited
        point.retried += retried
        point.dead += dead
        self._prune(bucket)

    async def record_backlog_and_rate(
        self, bucket: int, scope: str, backlog: int, rate: float | None
    ) -> None:
        point = self._points.setdefault((bucket, scope), _empty_point(bucket, scope))
        point.backlog = backlog
        point.rate = rate
        self._prune(bucket)

    async def query(
        self, scope: str, start_bucket: int, end_bucket: int
    ) -> list[StatsPoint]:
        return [
            point.model_copy()
            for (bucket, point_scope), point in sorted(self._points.items())
            if point_scope == scope and start_bucket <= bucket <= end_bucket
        ]

    def _prune(self, current_bucket: int) -> None:
        horizon = current_bucket - self._retention
        stale = [key for key in self._points if key[0] < horizon]
        for key in stale:
            del self._points[key]

    async def close(self) -> None:
        self._points.clear()


_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS per_second_stats (
    bucket    BIGINT      NOT NULL,
    scope     VARCHAR(128) NOT NULL,
    requested INTEGER     NOT NULL DEFAULT 0,
    succeeded INTEGER     NOT NULL DEFAULT 0,
    limited   INTEGER     NOT NULL DEFAULT 0,
    retried   INTEGER     NOT NULL DEFAULT 0,
    dead      INTEGER     NOT NULL DEFAULT 0,
    backlog   INTEGER     NOT NULL DEFAULT 0,
    rate      DOUBLE PRECISION,
    PRIMARY KEY (bucket, scope)
);
"""

_UPSERT_SQL = """
INSERT INTO per_second_stats
    (bucket, scope, requested, succeeded, limited, retried, dead, backlog, rate)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (bucket, scope) DO UPDATE SET
    requested = per_second_stats.requested + EXCLUDED.requested,
    succeeded = per_second_stats.succeeded + EXCLUDED.succeeded,
    limited   = per_second_stats.limited + EXCLUDED.limited,
    retried   = per_second_stats.retried + EXCLUDED.retried,
    dead      = per_second_stats.dead + EXCLUDED.dead,
    backlog   = EXCLUDED.backlog,
    rate      = EXCLUDED.rate;
"""

_QUERY_SQL = """
SELECT bucket, scope, requested, succeeded, limited, retried, dead, backlog, rate
FROM per_second_stats
WHERE scope = $1 AND bucket BETWEEN $2 AND $3
ORDER BY bucket;
"""


class PostgresStats:
    """PostgreSQL 按秒统计，计数器 upsert 累加、积压/速率每秒覆盖。"""

    def __init__(self, database_url: str) -> None:
        self._url = database_url
        self._pool: object | None = None

    async def init(self) -> None:
        import asyncpg  # 延迟导入：无 PG 环境也能跑内存模式

        self._pool = await asyncpg.create_pool(self._url, min_size=1, max_size=5)
        async with self._pool.acquire() as conn:  # type: ignore[union-attr]
            await conn.execute(_CREATE_TABLE_SQL)

    async def increment(
        self,
        bucket: int,
        scope: str,
        *,
        requested: int = 0,
        succeeded: int = 0,
        limited: int = 0,
        retried: int = 0,
        dead: int = 0,
    ) -> None:
        async with self._pool.acquire() as conn:  # type: ignore[union-attr]
            await conn.execute(
                _UPSERT_SQL,
                bucket,
                scope,
                requested,
                succeeded,
                limited,
                retried,
                dead,
                0,
                None,
            )

    async def record_backlog_and_rate(
        self, bucket: int, scope: str, backlog: int, rate: float | None
    ) -> None:
        async with self._pool.acquire() as conn:  # type: ignore[union-attr]
            await conn.execute(
                _UPSERT_SQL, bucket, scope, 0, 0, 0, 0, 0, backlog, rate
            )

    async def query(
        self, scope: str, start_bucket: int, end_bucket: int
    ) -> list[StatsPoint]:
        async with self._pool.acquire() as conn:  # type: ignore[union-attr]
            rows = await conn.fetch(_QUERY_SQL, scope, start_bucket, end_bucket)
        return [
            StatsPoint(
                bucket=row["bucket"],
                scope=row["scope"],
                requested=row["requested"],
                succeeded=row["succeeded"],
                limited=row["limited"],
                retried=row["retried"],
                dead=row["dead"],
                backlog=row["backlog"],
                rate=row["rate"],
            )
            for row in rows
        ]

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()  # type: ignore[union-attr]
            self._pool = None


def build_stats(database_url: str) -> StatsSink:
    if database_url.strip():
        return PostgresStats(database_url)
    return MemoryStats()


def now_bucket() -> int:
    return int(time.time())
