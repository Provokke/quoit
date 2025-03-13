from __future__ import annotations

import random
from dataclasses import dataclass


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 1
    base_delay: float = 0.0
    factor: float = 2.0
    max_delay: float = 60.0
    jitter: float = 0.0
    retry_on: tuple[type[BaseException], ...] = (Exception,)
    give_up_on: tuple[type[BaseException], ...] = ()

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if self.base_delay < 0 or self.max_delay < 0:
            raise ValueError("delays cannot be negative")
        if self.factor < 1:
            raise ValueError("factor must be at least 1")
        if not 0 <= self.jitter <= 1:
            raise ValueError("jitter must be between 0 and 1")

    @classmethod
    def none(cls) -> RetryPolicy:
        return cls(max_attempts=1)

    @classmethod
    def fixed(cls, attempts: int, delay: float) -> RetryPolicy:
        return cls(max_attempts=attempts, base_delay=delay, factor=1.0)

    @classmethod
    def exponential(cls, attempts: int, base_delay: float, factor: float = 2.0, max_delay: float = 60.0) -> RetryPolicy:
        return cls(max_attempts=attempts, base_delay=base_delay, factor=factor, max_delay=max_delay)

    def should_retry(self, error: BaseException, attempts_made: int) -> bool:
        if attempts_made >= self.max_attempts:
            return False
        if isinstance(error, self.give_up_on):
            return False
        return isinstance(error, self.retry_on)

    def delay_for(self, attempts_made: int, rng: random.Random | None = None) -> float:
        if attempts_made < 1:
            raise ValueError("attempts_made must be at least 1")
        delay = min(self.max_delay, self.base_delay * self.factor ** (attempts_made - 1))
        if self.jitter and delay:
            source = rng or random
            delay *= 1 + self.jitter * (source.random() * 2 - 1)
        return max(0.0, min(delay, self.max_delay))
