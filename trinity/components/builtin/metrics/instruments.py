from typing import Any

from pyformance.meters import (
    Histogram,
    Meter,
    Timer,
)


class NoopHistogram(Histogram):

    def __init__(self) -> None:
        pass

    def add(self, value: float) -> None:
        pass

    def clear(self) -> None:
        pass

    def get_count(self) -> float:
        return 0.0

    def get_sum(self) -> float:
        return 0.0

    def get_max(self) -> float:
        return 0.0

    def get_min(self) -> float:
        return 0.0

    def get_mean(self) -> int:
        return 0

    def get_stddev(self) -> int:
        return 0

    def get_var(self) -> int:
        return 0

    def get_snapshot(self) -> None:
        raise NotImplementedError("`get_snapshot` on NoopHistogram isn't implemented")


class NoopMeter(Meter):

    def __init__(self) -> None:
        pass

    def clear(self) -> None:
        pass

    def get_one_minute_rate(self) -> int:
        return 0

    def get_five_minute_rate(self) -> int:
        return 0

    def get_fifteen_minute_rate(self) -> int:
        return 0

    def tick(self) -> None:
        pass

    def mark(self, value: int = 1) -> None:
        pass

    def get_count(self) -> float:
        return 0.0

    def get_mean_rate(self) -> int:
        return 0


class NoopTimerContext:

    def __enter__(self) -> None:
        pass

    def __exit__(self, t, v, tb):  # type: ignore
        pass


class NoopTimer(Timer):

    def __init__(self) -> None:
        pass

    def get_count(self) -> float:
        return 0.0

    def get_sum(self) -> float:
        return 0.0

    def get_max(self) -> float:
        return 0.0

    def get_min(self) -> float:
        return 0.0

    def get_mean(self) -> float:
        return 0.0

    def get_stddev(self) -> float:
        return 0.0

    def get_var(self) -> float:
        return 0.0

    def get_snapshot(self) -> float:
        return 0.0

    def get_mean_rate(self) -> float:
        return 0.0

    def get_one_minute_rate(self) -> int:
        return 0

    def get_five_minute_rate(self) -> int:
        return 0

    def get_fifteen_minute_rate(self) -> int:
        return 0

    def time(self, *args: Any, **kwargs: Any) -> NoopTimerContext:
        return NoopTimerContext()

    def clear(self) -> None:
        pass
