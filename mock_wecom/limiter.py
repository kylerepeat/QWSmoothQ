"""简单的内存令牌桶（Mock 服务专用）。"""

from __future__ import annotations

import threading


class TokenBucket:
    """容量固定为 1 的匀速令牌桶。

    初始令牌为 0、时钟戳锚定在"一个补票周期之前"，因此首次请求恰好补满 1 个
    令牌并放行；之后严格按 ``1/rate`` 的间隔匀速放行，从机制上杜绝突发。
    被拒不推进时钟，调用方需等满一个补票周期后再试。
    """

    def __init__(self, rate: float, capacity: float = 1.0) -> None:
        self.rate = rate
        self.capacity = capacity
        self._tokens = 0.0
        self._updated: float | None = None
        self._lock = threading.Lock()

    def try_acquire(self, now: float) -> bool:
        with self._lock:
            if self._updated is None:
                # 锚定到一个补票周期之前：首次调用按 elapsed 恰好补满
                self._updated = now - self.capacity / self.rate
            elapsed = max(0.0, now - self._updated)
            self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
            # 1e-9 的容差吸收浮点表示误差
            if self._tokens >= 1.0 - 1e-9:
                self._tokens -= 1.0
                # 只有取到令牌才推进时钟；被拒时保留时间戳，
                # 否则流逝时间会被这次拒绝吞掉，下次补不满一个票
                self._updated = now
                return True
            return False
