import asyncio
from argparse import (
    ArgumentParser,
    _SubParsersAction,
)
from typing import (
    NewType,
)

from eth_keys.datatypes import PrivateKey


from eth2.beacon.chains.base import BeaconChain
from eth2.beacon.state_machines.forks.serenity.blocks import (
    SerenityBeaconBlock,
)
from eth2.beacon.state_machines.base import BaseBeaconStateMachine  # noqa: F401
from eth2.beacon.tools.builder.proposer import (
    _get_proposer_index,
    create_block_on_state,
)
from eth2.beacon.types.blocks import BaseBeaconBlock
from eth2.beacon.types.states import BeaconState
from eth2.beacon.typing import (
    Slot,
)

from p2p import ecies
from p2p.constants import DEFAULT_MAX_PEERS
from trinity._utils.shutdown import (
    exit_with_service_and_endpoint,
)
from trinity.config import (
    BeaconAppConfig,
    BeaconChainConfig,
)
from trinity.endpoint import TrinityEventBusEndpoint
from trinity.extensibility import BaseIsolatedPlugin
from trinity.protocol.bcc.peer import BCCPeerPool
from trinity.server import BCCServer
from trinity.sync.beacon.chain import BeaconChainSyncer

from trinity.db.beacon.manager import (
    create_db_consumer_manager
)
from trinity.plugins.eth2.beacon.testing_blocks_generators import (
    get_ten_blocks_context,
)

from eth2.beacon.configs import BeaconConfig
from trinity.plugins.eth2.beacon.testing_blocks_generators import config as testing_config


class BeaconNodePlugin(BaseIsolatedPlugin):

    @property
    def name(self) -> str:
        return "Beacon Node"

    def configure_parser(self, arg_parser: ArgumentParser, subparser: _SubParsersAction) -> None:
        arg_parser.add_argument(
            "--bootstrap_nodes",
            help="enode://node1@0.0.0.0:1234,enode://node2@0.0.0.0:5678",
        )
        arg_parser.add_argument(
            "--beacon-nodekey",
            help="0xabcd",
        )
        arg_parser.add_argument(
            "--mock-blocks",
            type=bool, required=False,
        )

    def on_ready(self, manager_eventbus: TrinityEventBusEndpoint) -> None:
        if self.context.trinity_config.has_app_config(BeaconAppConfig):
            self.start()

    def do_start(self) -> None:

        args = self.context.args
        trinity_config = self.context.trinity_config
        beacon_config = trinity_config.get_app_config(BeaconAppConfig)

        db_manager = create_db_consumer_manager(trinity_config.database_ipc_path)
        base_db = db_manager.get_db()  # type: ignore
        chain_db = db_manager.get_chaindb()  # type: ignore
        chain_config = beacon_config.get_chain_config()
        chain = (chain_config.beacon_chain_class)(base_db)

        if self.context.args.beacon_nodekey:
            from eth_keys.datatypes import PrivateKey
            privkey = PrivateKey(bytes.fromhex(self.context.args.beacon_nodekey))
        else:
            privkey = ecies.generate_privkey()

        server = BCCServer(
            privkey=privkey,
            port=self.context.args.port,
            chain=chain,
            chaindb=chain_db,
            headerdb=None,
            base_db=base_db,
            network_id=trinity_config.network_id,
            max_peers=DEFAULT_MAX_PEERS,
            bootstrap_nodes=None,
            preferred_nodes=None,
            event_bus=self.context.event_bus,
            token=None,
        )

        syncer = BeaconChainSyncer(
            chain_db,
            server.peer_pool,
            server.cancel_token,
        )

        validator = Validator(
            beacon_config=testing_config,
            chain=chain,
            peer_pool=server.peer_pool,
            privkey=privkey,
        )

        slot_ticker = SlotTicker(
            genesis_slot=testing_config.GENESIS_SLOT,
            genesis_time=chain_config.genesis_time,
            seconds_per_slot=6,
            validator=validator,
            state_machine=chain.get_state_machine(at_block=chain.get_canonical_head()),
        )

        loop = asyncio.get_event_loop()
        asyncio.ensure_future(exit_with_service_and_endpoint(server, self.context.event_bus))

        blocks = get_ten_blocks_context(chain, args.mock_blocks)

        if args.mock_blocks:
            asyncio.ensure_future(
                chain_db.coro_persist_block_chain(blocks, SerenityBeaconBlock)
            )

        asyncio.ensure_future(server.run())
        asyncio.ensure_future(syncer.run())
        asyncio.ensure_future(slot_ticker._job())
        loop.run_forever()
        loop.close()


class Validator:
    """
    Reference: https://github.com/ethereum/trinity/blob/master/eth2/beacon/tools/builder/proposer.py#L175  # noqa: E501
    """

    def __init__(
            self,
            beacon_config: BeaconConfig,
            chain: BeaconChain,
            peer_pool: BCCPeerPool,
            privkey: PrivateKey) -> None:
        self.beacon_config = beacon_config
        self.chain = chain
        self.peer_pool = peer_pool
        self.privkey = privkey

    def new_slot(self, slot: int) -> None:
        """
        The callback for `SlotTicker`, to be called whenever new slot is ticked.
        """
        print("New slot", slot)
        self.propose_block(slot=slot)

    def propose_block(self, slot: int) -> None:
        block = self._make_proposing_block(slot)
        # TODO: broadcast the block to the peers in `self.peer_pool`

    def _get_caononical_head(self) -> BaseBeaconBlock:
        return self.chain.get_canonical_head()

    def _get_head_state_machine(self) -> BaseBeaconStateMachine:
        return self.chain.get_state_machine(at_block=self._get_caononical_head())

    def _get_head_state(self) -> BeaconState:
        return self._get_head_state_machine().state

    def _make_proposing_block(self, slot: int) -> BaseBeaconBlock:
        parent_block = self._get_caononical_head()
        state_machine = self._get_head_state_machine()
        state = self._get_head_state()
        proposer_index = _get_proposer_index(
            state_machine,
            state,
            slot,
            parent_block.root,
            self.beacon_config,
        )
        return create_block_on_state(
            state=state,
            config=self.beacon_config,
            state_machine=state_machine,
            block_class=self.chain.get_block_class(),
            parent_block=parent_block,
            slot=slot,
            validator_index=proposer_index,
            privkey=self.privkey,
            attestations=[],
        )


async def _get_current_slot(genesis_slot, genesis_time, seconds_per_slot):
    import time
    now = int(time.time())
    return max((now - genesis_time) // seconds_per_slot, 0) + genesis_slot


class SlotTicker:
    _genesis_time: int
    _seconds_per_slot: int
    _validator: Validator
    _state_machine: BaseBeaconStateMachine
    _task: asyncio.Task
    latest_slot: int

    def __init__(
            self,
            genesis_slot: Slot,
            genesis_time: int,
            seconds_per_slot: int,
            validator: Validator,
            state_machine: BaseBeaconStateMachine) -> None:
        self._genesis_slot = genesis_slot
        self._genesis_time = genesis_time
        self._seconds_per_slot = seconds_per_slot
        self._validator = validator
        self._state_machine = state_machine
        self.latest_slot = 0

    async def _job(self) -> None:
        while True:
            await asyncio.sleep(self._seconds_per_slot)
            slot = await _get_current_slot(
                self._genesis_slot,
                self._genesis_time,
                self._seconds_per_slot,
            )
            if slot > self.latest_slot:
                self.latest_slot = slot
                self._validator.new_slot(slot)
                # self._state_machine.new_slot(slot)

    def cancel(self) -> None:
        self._task.cancel()
