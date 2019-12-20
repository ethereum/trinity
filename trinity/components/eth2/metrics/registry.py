from prometheus_client import CollectorRegistry, Gauge, Counter


class AllMetrics():
    def __init__(self, registry: CollectorRegistry) -> None:
        #
        # Interop
        #

        # libp2p_peers
        self.libp2p_peers = Gauge('libp2p_peers', 'Tracks number of libp2p peers', registry=registry)  # noqa: E501

        # On slot transition
        self.beacon_slot = Gauge('beacon_slot', 'Latest slot of the beacon chain state', registry=registry)  # noqa: E501

        # On block transition
        self.beacon_head_slot = Gauge('beacon_head_slot', 'Slot of the head block of the beacon chain', registry=registry)  # noqa: E501
        self.beacon_head_root = Gauge('beacon_head_root', 'Root of the head block of the beacon chain', registry=registry)  # noqa: E501

        # On epoch transition
        self.beacon_previous_justified_epoch = Gauge('beacon_previous_justified_epoch', 'Current previously justified epoch', registry=registry)  # noqa: E501
        self.beacon_previous_justified_root = Gauge('beacon_previous_justified_root', 'Current previously justified root', registry=registry)  # noqa: E501
        self.beacon_current_justified_epoch = Gauge('beacon_current_justified_epoch', 'Current justified epoch', registry=registry)  # noqa: E501
        self.beacon_current_justified_root = Gauge('beacon_current_justified_root', 'Current justified root', registry=registry)  # noqa: E501
        self.beacon_finalized_epoch = Gauge('beacon_finalized_epoch', 'Current finalized epoch', registry=registry)  # noqa: E501
        self.beacon_finalized_root = Gauge('beacon_finalized_root', 'Current finalized root', registry=registry)  # noqa: E501

        #
        # Other
        #

        # Validator
        self.validator_proposed_blocks = Counter("validator_proposed_blocks", "counter of proposed blocks", registry=registry)  # noqa: E501
        self.validator_sent_attestation = Counter("validator_sent_attestation", "counter of attested", registry=registry)  # noqa: E501


registry = CollectorRegistry()
metrics = AllMetrics(registry)
