import time
from types import ModuleType
from typing import Dict, Any

from pyformance import MetricsRegistry


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
