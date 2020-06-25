import time
from typing import Any, Collection, Type

from eth.db.atomic import AtomicDB
import factory

from eth2.beacon.chains.base import BaseBeaconChain
from eth2.beacon.chains.testnet import SkeletonLakeChain
from eth2.beacon.fork_choice.higher_slot import HigherSlotScoring
from eth2.beacon.state_machines.forks.serenity.blocks import (
    SerenityBeaconBlock,
    SerenitySignedBeaconBlock,
)
from eth2.beacon.state_machines.forks.skeleton_lake.configs import (
    MINIMAL_SERENITY_CONFIG,
)
from eth2.beacon.tools.builder.initializer import create_mock_genesis
from eth2.beacon.tools.builder.validator import mk_keymap_of_size
from eth2.beacon.tools.misc.ssz_vector import override_lengths
from eth2.beacon.types.blocks import BaseSignedBeaconBlock
from eth2.beacon.types.states import BeaconState
from eth2.beacon.typing import Timestamp


class BeaconChainFactory(factory.Factory):
    num_validators = 8
    config = MINIMAL_SERENITY_CONFIG

    branch: Collection[BaseSignedBeaconBlock] = None
    genesis_state: BeaconState = None
    genesis_block: SerenityBeaconBlock = None

    class Meta:
        model = SkeletonLakeChain

    @classmethod
    def _create(
        cls, model_class: Type[BaseBeaconChain], *args: Any, **kwargs: Any
    ) -> BaseBeaconChain:
        """
        Create a BeaconChain according to the factory definition.

        NOTE: clients of this class may provide a ``branch`` keyword in the ``kwargs`` to
        construct a chain with a ``Collection[BaseSignedBeaconBlock]``. This ``branch`` is NOT
        assumed to have been constructed according to the full set of validity rules, e.g.
        lacking a proper signature so the ``perform_validation`` option to ``import_block``
        is disabled.
        """
        override_lengths(cls.config)
        if "num_validators" in kwargs:
            num_validators = kwargs["num_validators"]
        else:
            num_validators = cls.num_validators

        if kwargs["genesis_state"] is None:
            keymap = mk_keymap_of_size(num_validators)
            genesis_state, _ = create_mock_genesis(
                config=cls.config,
                pubkeys=tuple(keymap.keys()),
                keymap=keymap,
                genesis_block_class=SerenityBeaconBlock,
                genesis_time=Timestamp(int(time.time())),
            )
        else:
            genesis_state = kwargs["genesis_state"]

        db = kwargs.pop("db", AtomicDB())
        chain = model_class.from_genesis(base_db=db, genesis_state=genesis_state)

        if kwargs["branch"] is not None:
            branch = kwargs["branch"]
            for block in branch:
                if block.is_genesis:
                    continue
                # NOTE: ideally we use the ``import_block`` method
                # on ``chain`` but for the time being we skip some
                # validation corresponding to assumptions made in clients of
                # this class. A future refactoring should use the external API.
                chain.chaindb.persist_block(
                    block, SerenitySignedBeaconBlock, HigherSlotScoring()
                )

        return chain
