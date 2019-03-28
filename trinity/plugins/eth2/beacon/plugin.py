from trinity.config import BeaconChainConfig
from eth2.beacon.types.blocks import (
    BeaconBlock,
)
from eth2.beacon.tools.builder.proposer import (
    _get_proposer_index,
    create_block_on_state,
)
from eth2.beacon.chains.base import BeaconChain
from eth_keys.datatypes import PrivateKey
import asyncio
from argparse import (
    ArgumentParser,
    _SubParsersAction,
)
from typing import (
    NewType,
)

from p2p import ecies
from p2p.constants import DEFAULT_MAX_PEERS
from trinity._utils.shutdown import (
    exit_with_service_and_endpoint,
)
from trinity.config import BeaconAppConfig
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
from eth2.beacon.tools.builder.proposer import create_block_on_state

from eth2.beacon.state_machines.forks.serenity.blocks import (
    SerenityBeaconBlock,
)


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
        chain = chain_config.beacon_chain_class(base_db)

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

        loop = asyncio.get_event_loop()
        asyncio.ensure_future(exit_with_service_and_endpoint(server, self.context.event_bus))

        blocks = get_ten_blocks_context(chain, args.mock_blocks)

        if args.mock_blocks:
            asyncio.ensure_future(
                chain_db.coro_persist_block_chain(blocks, SerenityBeaconBlock)
            )

        asyncio.ensure_future(server.run())
        asyncio.ensure_future(syncer.run())
        loop.run_forever()
        loop.close()


SlotTicker = NewType('SlotTicker', object)


class Validator:
    """
    Reference: https://github.com/ethereum/trinity/blob/master/eth2/beacon/tools/builder/proposer.py#L175  # noqa: E501
    """
    beacon_config: BeaconChainConfig
    chain: BeaconChain
    peer_pool: BCCPeerPool
    privkey: PrivateKey
    slot_ticker: SlotTicker

    def __init__(
            self,
            beacon_config: BeaconChainConfig,
            chain: BeaconChain,
            peer_pool: BCCPeerPool,
            privkey: PrivateKey,
            slot_ticker: SlotTicker) -> None:
        self.beacon_config = beacon_config
        self.chain = chain
        self.peer_pool = peer_pool
        self.privkey = privkey
        self.slot_ticker = slot_ticker

    def propose_block(self):
        block = self._make_proposing_block()
        # TODO: broadcast the block to the peers in `self.peer_pool`

    def _make_proposing_block(self) -> BeaconBlock:
        parent_block = self.chain.get_canonical_head()
        slot = self.slot_ticker.next_slot()
        state_machine = self.chain.get_state_machine()
        proposer_index = _get_proposer_index(
            state_machine,
            self.chain.state,
            slot,
            parent_block.root,
            self.beacon_config,
        )
        return create_block_on_state(
            state=self.chain.state,
            config=self.beacon_config,
            state_machine=state_machine,
            block_class=self.chain.get_block_class(),
            parent_block=parent_block,
            slot=self.slot_ticker.next_slot(),
            validator_index=proposer_index,
            privkey=self.privkey,
            attestations=[],
        )
