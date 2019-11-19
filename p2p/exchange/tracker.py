from abc import abstractmethod
from typing import Optional

from eth_utils import get_extended_debug_logger

from p2p.stats.ema import EMA
from p2p.stats.percentile import Percentile
from p2p.stats.stddev import StandardDeviation

from .abc import PerformanceTrackerAPI
from .constants import ROUND_TRIP_TIMEOUT
from .typing import TRequestCommand, TResult


class BasePerformanceTracker(PerformanceTrackerAPI[TRequestCommand, TResult]):
    logger = get_extended_debug_logger('trinity.protocol.common.trackers.PerformanceTracker')

    def __init__(self) -> None:
        self.total_msgs = 0
        self.total_items = 0
        self.total_timeouts = 0
        self.total_response_time = 0.0

        # a percentage between 0-100 for how much of the requested
        # data the peer typically returns with 100 meaning they consistently
        # return all of the data we request and 0 meaning they only return
        # empty responses.
        self.response_quality_ema = EMA(initial_value=0, smoothing_factor=0.05)

        # Metrics for the round trip request/response time
        self.round_trip_ema = EMA(initial_value=ROUND_TRIP_TIMEOUT, smoothing_factor=0.05)
        self.round_trip_99th = Percentile(percentile=0.99, window_size=200)
        self.round_trip_stddev = StandardDeviation(window_size=200)

        # an EMA of the items per second
        self.items_per_second_ema = EMA(initial_value=0, smoothing_factor=0.05)

    def get_stats(self) -> str:
        """
        Return a human readable string representing the stats for this tracker.
        """
        if not self.total_msgs:
            return 'None'

        try:
            rt99 = self.round_trip_99th.value
        except ValueError:
            rt99 = 0

        try:
            rt_stddev = self.round_trip_stddev.value
        except ValueError:
            rt_stddev = 0

        # msgs: total number of messages
        # items: total number of items
        # rtt: round-trip-time (ema/99th/stddev)
        # ips: items-per-second (ema)
        # timeouts: total number of timeouts
        # missing: total number of missing response items
        # quality: 0-100 for how complete responses are
        return (
            f"msgs={self.total_msgs}  items={self.total_items}  "
            f"rtt={self.round_trip_ema.value:.2f}/{rt99:.2f}/{rt_stddev:.2f}  "
            f"ips={self.items_per_second_ema.value:.5f}  "
            f"timeouts={self.total_timeouts}  quality={int(self.response_quality_ema.value)}"
        )

    @abstractmethod
    def _get_request_size(self, request: TRequestCommand) -> Optional[int]:
        """
        The request size represents the number of *things* that were requested,
        not taking into account the sizes of individual items.

        Some requests cannot be used to determine the expected size.  In this
        case `None` should be returned.  (Specifically the `GetBlockHeaders`
        anchored to a block hash.
        """
        ...

    @abstractmethod
    def _get_result_size(self, result: TResult) -> int:
        """
        The result size represents the number of *things* that were returned,
        not taking into account the sizes of individual items.
        """
        ...

    @abstractmethod
    def _get_result_item_count(self, result: TResult) -> int:
        """
        The item count is intended to more accurately represent the size of the
        response, taking into account things like the size of individual
        response items such as the number of transactions in a block.
        """
        ...

    def record_timeout(self, timeout: float) -> None:
        self.total_msgs += 1
        self.total_timeouts += 1
        self.response_quality_ema.update(0)
        self.round_trip_ema.update(timeout)
        self.round_trip_99th.update(timeout)
        self.round_trip_stddev.update(timeout)
        self.items_per_second_ema.update(0)

    def record_response(self,
                        elapsed: float,
                        request: TRequestCommand,
                        result: TResult) -> None:
        self.total_msgs += 1

        request_size = self._get_request_size(request)
        response_size = self._get_result_size(result)
        num_items = self._get_result_item_count(result)

        if request_size is None:
            # In the event that request size cannot be determined we skip stats
            # tracking based on request size.
            pass

        elif response_size > request_size:
            self.logger.warning(
                "%s got oversized response.  requested: %d  received: %d",
                type(self).__name__,
                request_size,
                response_size,
            )
        else:
            if request_size == 0:
                self.logger.warning(
                    "%s encountered request for zero items. This should never happen",
                    type(self).__name__,
                )
                # we intentionally don't update the ema here since this is an
                # odd and unexpected case.
            elif response_size == 0:
                self.response_quality_ema.update(0)
            else:
                percent_returned = 100 * response_size / request_size
                self.response_quality_ema.update(percent_returned)

        self.total_items += num_items
        self.total_response_time += elapsed

        self.round_trip_ema.update(elapsed)
        self.round_trip_99th.update(elapsed)
        self.round_trip_stddev.update(elapsed)

        if elapsed > 0:
            throughput = num_items / elapsed
            self.items_per_second_ema.update(throughput)
        else:
            self.logger.warning(
                "%s encountered response time of zero.  This should never happen",
                type(self).__name__,
            )
