import time
from types import ModuleType
from typing import Dict, Any, Union

from pyformance import MetricsRegistry
from pyformance.meters import (
    SimpleGauge,
    Counter,
)

from trinity.components.builtin.metrics.instruments import (
    NoopHistogram,
    NoopMeter,
    NoopTimer,
)


class HostMetricsRegistry(MetricsRegistry):

    def __init__(self, host: str, clock: ModuleType = time) -> None:
        super().__init__(clock)
        self.host = host

    def dump_metrics(self) -> Dict[str, Dict[str, Any]]:
        metrics = super().dump_metrics()

        for key in metrics:
            # We want every metric to include a 'host' identifier to be able to filter accordingly
            metrics[key]['host'] = self.host

        return metrics


NOOP_COUNTER = Counter()
NOOP_TIMER = NoopTimer()
NOOP_METER = NoopMeter()
NOOP_GAUGE = SimpleGauge()
NOOP_HISTOGRAM = NoopHistogram()

Instrument = Union[Counter, NoopTimer, NoopMeter, SimpleGauge, NoopHistogram]


class NoopMetricsRegistry(HostMetricsRegistry):
    """
    A ``MetricsRegistry`` where every action will result in a noop. Used when metrics are disabled.
    """

    def __init__(self) -> None:
        pass

    def dump_metrics(self) -> Dict[str, Dict[str, Any]]:
        pass

    def add(self, key: str, metric: Instrument) -> None:
        pass

    def counter(self, key: str) -> Counter:
        return NOOP_COUNTER

    def histogram(self, key: str) -> NoopHistogram:
        return NOOP_HISTOGRAM

    def gauge(self,
              key: str,
              gauge: SimpleGauge = None,
              default: SimpleGauge = None) -> SimpleGauge:
        return NOOP_GAUGE

    def meter(self, key: str) -> NoopMeter:
        return NOOP_METER

    def timer(self, key: str) -> NoopTimer:
        return NOOP_TIMER
