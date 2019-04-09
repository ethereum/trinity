import asyncio
import time
import logging
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
    Second,
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
from trinity.sync.common.chain import (
    SyncBlockImporter,
)
from trinity.plugins.eth2.beacon.testing_blocks_generators import (
    get_ten_blocks_context,
)
from trinity.plugins.eth2.beacon.testing_config import (
    index_to_pubkey,
    keymap,
)
from eth2.beacon.chains.base import (
    BaseBeaconChain,
)
from trinity._utils.shellart import (
    bold_green,
    bold_red,
)
from lahja import (
    BaseEvent,
    BroadcastConfig,
)

from p2p.service import BaseService

from cancel_token import (
    CancelToken,
)


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
            SyncBlockImporter(chain),
            server.cancel_token,
        )

        validator_privkey = keymap[index_to_pubkey[self.context.args.validator_index]]
        validator = Validator(
            validator_index=self.context.args.validator_index,
            chain=chain,
            peer_pool=server.peer_pool,
            privkey=validator_privkey,
            event_bus=self.context.event_bus,
            token=server.cancel_token,
        )

        slot_ticker = SlotTicker(
            genesis_slot=chain_config.genesis_slot,
            genesis_time=chain_config.genesis_time,
            chain=chain,
            event_bus=self.context.event_bus,
            token=server.cancel_token,
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
        asyncio.ensure_future(slot_ticker.run())
        asyncio.ensure_future(validator.run())
        loop.run_forever()
        loop.close()


class NewSlotEvent(BaseEvent):
    def __init__(self, slot: Slot, elapse_time: Second):
        self.slot = slot
        self.elapse_time = elapse_time


class Validator(BaseService):
    """
    Reference: https://github.com/ethereum/trinity/blob/master/eth2/beacon/tools/builder/proposer.py#L175  # noqa: E501
    """

    validator_index: int
    chain: BeaconChain
    peer_pool: BCCPeerPool
    privkey: PrivateKey
    event_bus: TrinityEventBusEndpoint

    logger = logging.getLogger(f'trinity.plugins.eth2.beacon.Validator')

    def __init__(
            self,
            validator_index: int,
            chain: BeaconChain,
            peer_pool: BCCPeerPool,
            privkey: PrivateKey,
            event_bus: TrinityEventBusEndpoint,
            token: CancelToken = None) -> None:
        super().__init__(token)
        self.validator_index = validator_index
        self.chain = chain
        self.peer_pool = peer_pool
        self.privkey = privkey
        self.event_bus = event_bus

    async def _run(self) -> None:
        await self.event_bus.wait_until_serving()
        self.logger.debug(bold_green("validator running!!!"))
        self.run_daemon_task(self.handle_new_slot())
        await self.cancellation()

    async def handle_new_slot(self) -> None:
        """
        The callback for `SlotTicker`, to be called whenever new slot is ticked.
        """
        async for event in self.event_bus.stream(NewSlotEvent):
            await self.new_slot(event.slot)

    async def new_slot(self, slot: int) -> None:
        head = self.chain.get_canonical_head()
        state_machine = self.chain.get_state_machine()
        state = state_machine.state
        self.logger.debug(
            bold_green(f"head: slot={head.slot}, state root={head.state_root}")
        )
        proposer_index = _get_proposer_index(
            state_machine,
            state,
            slot,
            head.root,
            state_machine.config,
        )
        if self.validator_index == proposer_index:
            self.propose_block(slot=slot, state=state,
                               state_machine=state_machine, head_block=head)
        else:
            self.skip_block(
                slot=slot,
                state=state,
                state_machine=state_machine,
                parent_block=head,
            )

    def propose_block(self,
                      slot: int,
                      state: BeaconState,
                      state_machine: BaseBeaconStateMachine,
                      head_block: BaseBeaconBlock) -> None:
        block = self._make_proposing_block(slot, state, state_machine, head_block)
        self.logger.debug(
            bold_green(f"!@# propose block={block}")
        )
        for _, peer in enumerate(self.peer_pool.connected_nodes.values()):
            self.logger.debug(
                bold_red(f"send block to: peer={peer}")
            )
            peer.sub_proto.send_new_block(block)
        self.chain.import_block(block)

    def _make_proposing_block(self,
                              slot: int,
                              state: BeaconState,
                              state_machine: BaseBeaconStateMachine,
                              parent_block: BaseBeaconBlock) -> BaseBeaconBlock:
        return create_block_on_state(
            state=state,
            config=state_machine.config,
            state_machine=state_machine,
            block_class=SerenityBeaconBlock,
            parent_block=parent_block,
            slot=slot,
            validator_index=self.validator_index,
            privkey=self.privkey,
            attestations=(),
            check_proposer_index=False,
        )

    def skip_block(self,
                   slot: int,
                   state: BeaconState,
                   state_machine: BaseBeaconStateMachine,
                   parent_block: BaseBeaconBlock) -> None:
        post_state = state_machine.state_transition.apply_state_transition_without_block(
            state,
            # TODO: Change back to `slot` instead of `slot + 1`.
            # Currently `apply_state_transition_without_block` only returns the post state
            # of `slot - 1`, so we increment it by one to get the post state of `slot`.
            slot + 1,
            parent_block.root,
        )
        self.logger.debug(
            bold_green(f"!@# skip block, post state={post_state.root}")
        )
        # FIXME: We might not need to persist state for skip slots since `create_block_on_state`
        # will run the state transition which also includes the state transition for skipped slots.
        self.chain.chaindb.persist_state(post_state)


class SlotTicker(BaseService):
    genesis_slot: Slot
    genesis_time: int
    chain: BaseBeaconChain
    latest_slot: int
    event_bus: TrinityEventBusEndpoint
    logger = logging.getLogger('SlotTicker')

    def __init__(
            self,
            genesis_slot: Slot,
            genesis_time: int,
            chain: BaseBeaconChain,
            event_bus: TrinityEventBusEndpoint,
            token: CancelToken = None) -> None:
        super().__init__(token)
        self._genesis_slot = genesis_slot
        self._genesis_time = genesis_time
        self.chain = chain
        self.latest_slot = 0
        self.event_bus = event_bus

    def get_seconds_per_slot(self):
        state_machine = self.chain.get_state_machine()
        return state_machine.config.SECONDS_PER_SLOT

    async def _run(self) -> None:
        self.run_daemon_task(self._keep_ticking())
        await self.cancellation()

    async def _keep_ticking(self) -> None:
        while self.is_operational:
            seconds_per_slot = self.get_seconds_per_slot()
            now = int(time.time())
            elapse_time = now - self._genesis_time
            if elapse_time >= (0 + seconds_per_slot):
                slot = elapse_time // seconds_per_slot + self._genesis_slot
                if slot > self.latest_slot:
                    self.logger.debug(
                        bold_green(f"New slot: {slot}\tElapse time: {elapse_time}")
                    )
                    self.latest_slot = slot
                    self.event_bus.broadcast(
                        NewSlotEvent(
                            slot=slot,
                            elapse_time=elapse_time,
                        ),
                        BroadcastConfig(internal=True),
                    )
            # don't sleep the full seconds_per_slot
            await asyncio.sleep(seconds_per_slot // 5)
