import factory

from p2p.capacity_limiter import CapacityLimiter


class CapacityLimiterFactory(factory.Factory):
    class Meta:
        model = CapacityLimiter

    num_tokens = 25
