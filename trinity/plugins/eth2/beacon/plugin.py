from argparse import (
    ArgumentParser,
    _SubParsersAction,
)
import asyncio

from typing import (
    Dict,
    Sequence,
)
from cancel_token import (
    CancelToken,
)
from eth_keys.datatypes import (
    PrivateKey,
)

from eth2.beacon.chains.base import (
    BeaconChain,
)
from eth2.beacon.state_machines.base import (
    BaseBeaconStateMachine,
)
from eth2.beacon.state_machines.forks.serenity.blocks import (
    SerenityBeaconBlock,
)
from eth2.beacon.tools.builder.proposer import (
    _get_proposer_index,
    create_block_on_state,
)
from eth2.beacon.types.blocks import (
    BaseBeaconBlock,
)
from eth2.beacon.types.states import (
    BeaconState,
)
from eth2.beacon.typing import (
    Slot,
    ValidatorIndex,
)
from p2p import ecies
from p2p.constants import (
    DEFAULT_MAX_PEERS,
)
from p2p.service import (
    BaseService,
)
from trinity._utils.shellart import (
    bold_green,
    bold_red,
)
from trinity._utils.shutdown import (
    exit_with_service_and_endpoint,
)
from trinity.config import (
    BeaconAppConfig,
)
from trinity.db.beacon.manager import (
    create_db_consumer_manager,
)
from trinity.endpoint import (
    TrinityEventBusEndpoint,
)
from trinity.extensibility import (
    BaseIsolatedPlugin,
)
from trinity.plugins.eth2.beacon.testing_blocks_generators import (
    get_ten_blocks_context,
)
from trinity.protocol.bcc.peer import (
    BCCPeerPool,
)
from trinity.server import (
    BCCServer,
)
from trinity.sync.beacon.chain import (
    BeaconChainSyncer,
)
from trinity.sync.common.chain import (
    SyncBlockImporter,
)

from .slot_ticker import (
    NewSlotEvent,
    SlotTicker,
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

        state_machine = chain.get_state_machine()
        state = state_machine.state
        registry_pubkeys = [v_record.pubkey for v_record in state.validator_registry]

        validator_indices = []
        validator_privkeys = {}
        for pubkey in trinity_config.validator_pubkeys:
            validator_index = registry_pubkeys.index(pubkey)
            validator_indices.append(validator_index)
            validator_privkeys[validator_index] = trinity_config.genesis_data.keymap[pubkey]

        validator = Validator(
            validator_indices=validator_indices,
            chain=chain,
            peer_pool=server.peer_pool,
            validator_privkeys=validator_privkeys,
            event_bus=self.context.event_bus,
            token=server.cancel_token,
        )

        slot_ticker = SlotTicker(
            genesis_slot=chain_config.genesis_slot,
            genesis_time=chain_config.genesis_time,
            seconds_per_slot=state_machine.config.SECONDS_PER_SLOT,
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


class Validator(BaseService):
    """
    Reference: https://github.com/ethereum/trinity/blob/master/eth2/beacon/tools/builder/proposer.py#L175  # noqa: E501
    """

    validator_indices: Sequence[ValidatorIndex]
    chain: BeaconChain
    peer_pool: BCCPeerPool
    validator_privkeys: Dict[ValidatorIndex, PrivateKey]
    event_bus: TrinityEventBusEndpoint

    def __init__(
            self,
            validator_indices: Sequence[ValidatorIndex],
            chain: BeaconChain,
            peer_pool: BCCPeerPool,
            validator_privkeys: Dict[ValidatorIndex, PrivateKey],
            event_bus: TrinityEventBusEndpoint,
            token: CancelToken = None) -> None:
        super().__init__(token)
        self.validator_indices = validator_indices
        self.chain = chain
        self.peer_pool = peer_pool
        self.validator_privkeys = validator_privkeys
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

    async def new_slot(self, slot: Slot) -> None:
        head = self.chain.get_canonical_head()
        state_machine = self.chain.get_state_machine()
        state = state_machine.state
        self.logger.debug(
            bold_green(f"head: slot={head.slot}, state root={head.state_root.hex()}")
        )
        proposer_index = _get_proposer_index(
            state,
            slot,
            state_machine.config,
        )
        if proposer_index in self.validator_indices:
            self.propose_block(
                proposer_index=proposer_index,
                slot=slot,
                state=state,
                state_machine=state_machine,
                head_block=head
            )
        else:
            self.skip_block(
                slot=slot,
                state=state,
                state_machine=state_machine,
                parent_block=head,
            )

    def propose_block(self,
                      proposer_index: ValidatorIndex,
                      slot: Slot,
                      state: BeaconState,
                      state_machine: BaseBeaconStateMachine,
                      head_block: BaseBeaconBlock) -> None:
        block = self._make_proposing_block(proposer_index, slot, state, state_machine, head_block)
        self.logger.debug(
            bold_green(f"Propose block={block}")
        )
        for _, peer in enumerate(self.peer_pool.connected_nodes.values()):
            self.logger.debug(
                bold_red(f"send block to: peer={peer}")
            )
            peer.sub_proto.send_new_block(block)
        self.chain.import_block(block)

    def _make_proposing_block(self,
                              proposer_index: ValidatorIndex,
                              slot: Slot,
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
            validator_index=proposer_index,
            privkey=self.validator_privkeys[proposer_index],
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
            Slot(slot + 1),
        )
        self.logger.debug(
            bold_green(f"Skip block, post state={post_state.root.hex()}")
        )
        # FIXME: We might not need to persist state for skip slots since `create_block_on_state`
        # will run the state transition which also includes the state transition for skipped slots.
        self.chain.chaindb.persist_state(post_state)
