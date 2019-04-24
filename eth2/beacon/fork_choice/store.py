class Store:
    def __init__(self, db: BaseBeaconChainDB, state: BeaconState, attestation_pool):
        self.db = db

        self._build_attestation_index(state, attestation_pool)

    def _build_attestation_index(self, state, attestation_pool):
        """
        Assembles a dictionary of latest attestation keyed by validator index.
        Any attestation made by a validator in the ``attestation_pool`` that occur after the last known attestation according to the DB take precedence.
        """
        # TODO
        # For `state.previous_epoch_attestations`, figure out the combination key (validator_index, timestamp) and map to a given attestation
        # For `state.current_epoch_attestations`, figure out the combination key (validator_index, timestamp) and map to a given attestation
        # For each attestation in `attestation_pool`, figure out the epoch, then figure out the validator index, then see if we have a later timestamp
        # If so, then overwrite the value in the index.
        self._attestation_index = {}

    def get_block_by_root(self, block_root: Hash32):
        return self.db.get_block_by_root(block_root)

    def get_previous_block(self, block):
        "Return the predecessor block in the chain from ``block``."
        # TODO
        pass

    def get_children(self, block):
        "Return the descendants of ``block`` in the chain."
        # TODO
        pass

    def get_ancestor(self, block, slot):
        "Return the block in the chain that is a predecessor of ``block`` at the requested ``slot``."
        # TODO
        pass


    def get_latest_attestation(self, index: ValidatorIndex) -> Attestation:
        """
        Return the latest attesation we know from the validator with the
        given ``index``.
        """
        return self._attestation_index[index]

    def get_latest_attestation_target(self, index: ValidatorIndex) -> BaseBeaconBlock:
        attestation = self.get_latest_attestation(index)
        target_block = self.get_block_by_root(attestation.target_root)
        return target_block
