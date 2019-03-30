import asyncio
from argparse import (
    ArgumentParser,
    _SubParsersAction,
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

from eth2.configs import Eth2Config
from trinity.plugins.eth2.beacon.testing_blocks_generators import (
    config as testing_config,
    index_to_pubkey,
    keymap,
)
import time


class BeaconNodePlugin(BaseIsolatedPlugin):

    @property
    def name(self) -> str:
        return "Beacon Node"

    def configure_parser(self, arg_parser: ArgumentParser, subparser: _SubParsersAction) -> None:
        arg_parser.add_argument(
            "--validator_index",
            type=int, required=True,
        )
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

        validator_privkey = keymap[index_to_pubkey[self.context.args.validator_index]]
        validator = Validator(
            validator_index=self.context.args.validator_index,
            eth2_config=testing_config,
            chain=chain,
            peer_pool=server.peer_pool,
            privkey=validator_privkey,
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
            validator_index: int,
            eth2_config: Eth2Config,
            chain: BeaconChain,
            peer_pool: BCCPeerPool,
            privkey: PrivateKey) -> None:
        self.validator_index = validator_index
        self.eth2_config = eth2_config
        self.chain = chain
        self.peer_pool = peer_pool
        self.privkey = privkey

    def new_slot(self, slot: int) -> None:
        """
        The callback for `SlotTicker`, to be called whenever new slot is ticked.
        """
        self.propose_block(slot=slot)

    def propose_block(self, slot: int) -> None:
        assert slot > self.eth2_config.GENESIS_SLOT
        head = self.chain.get_canonical_head()
        state_machine = self.chain.get_state_machine(at_block=head)
        state = state_machine.state
        proposer_index = _get_proposer_index(
            state_machine,
            state,
            slot,
            head.root,
            self.eth2_config,
        )
        if self.validator_index == proposer_index:
            block = self._make_proposing_block(slot, state, state_machine, head)
            for i, peer in enumerate(self.peer_pool.connected_nodes.values()):
                peer.sub_proto.send_blocks((block,), request_id=i)

    def _make_proposing_block(self,
                              slot: int,
                              state: BeaconState,
                              state_machine: BaseBeaconStateMachine,
                              parent_block: BaseBeaconBlock) -> BaseBeaconBlock:
        return create_block_on_state(
            state=state,
            config=self.eth2_config,
            state_machine=state_machine,
            block_class=SerenityBeaconBlock,
            parent_block=parent_block,
            slot=slot,
            validator_index=self.validator_index,
            privkey=self.privkey,
            attestations=[],
        )


def _get_current_slot(genesis_slot, genesis_time, seconds_per_slot):
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
        self.latest_slot = 0

    async def _job(self) -> None:
        while True:
            await asyncio.sleep(self._seconds_per_slot)
            slot = _get_current_slot(
                self._genesis_slot,
                self._genesis_time,
                self._seconds_per_slot,
            )
            if slot > self.latest_slot:
                print("new slot", slot)
                self.latest_slot = slot
                self._validator.new_slot(slot)

    def cancel(self) -> None:
        self._task.cancel()
