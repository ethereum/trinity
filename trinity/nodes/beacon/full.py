from dataclasses import dataclass
import logging
import time
from typing import Any, Collection, Iterable, Optional, Set, Tuple

from async_service import background_trio_service
from eth.db.backends.level import LevelDB
from eth.exceptions import BlockNotFound
from eth_keys.datatypes import PrivateKey
from eth_utils import humanize_hash
from libp2p.crypto.secp256k1 import create_new_key_pair
from libp2p.peer.id import ID as PeerID
from multiaddr import Multiaddr
import ssz
import trio
from trio_typing import TaskStatus

from eth2.api.http.validator import BlockBroadcasterAPI, Context
from eth2.api.http.validator import ServerHandlers as ValidatorAPIHandlers
from eth2.api.http.validator import SyncerAPI, SyncStatus
from eth2.beacon.chains.abc import BaseBeaconChain
from eth2.beacon.chains.exceptions import ParentNotFoundError, SlashableBlockError
from eth2.beacon.constants import GENESIS_SLOT
from eth2.beacon.db.abc import BaseBeaconChainDB
from eth2.beacon.helpers import compute_fork_digest, compute_start_slot_at_epoch
from eth2.beacon.types.blocks import BeaconBlock, SignedBeaconBlock
from eth2.beacon.types.states import BeaconState
from eth2.beacon.typing import Epoch, ForkDigest, Root, Slot
from eth2.clock import Clock, Tick, TimeProvider, get_unix_time
from eth2.configs import Eth2Config
from trinity._utils.trio_utils import JSONHTTPServer
from trinity.nodes.beacon.config import BeaconNodeConfig
from trinity.nodes.beacon.host import Host
from trinity.nodes.beacon.metadata import MetaData
from trinity.nodes.beacon.metadata import SeqNumber as MetaDataSeqNumber
from trinity.nodes.beacon.request_responder import GoodbyeReason
from trinity.nodes.beacon.status import Status

SYNC_REATTEMPT_COUNT = 10


def _mk_clock(
    config: Eth2Config, genesis_time: int, time_provider: TimeProvider
) -> Clock:
    return Clock(
        config.SECONDS_PER_SLOT,
        genesis_time,
        config.SLOTS_PER_EPOCH,
        config.SECONDS_PER_SLOT * config.SLOTS_PER_EPOCH,
        time_provider,
        # node can wait until genesis
        genesis_lookahead=0,
    )


def _mk_syncer() -> SyncerAPI:
    class _sync(SyncerAPI):
        async def get_status(self) -> SyncStatus:
            return SyncStatus(False, Slot(0), Slot(0), Slot(0))

    return _sync()


def _mk_validator_api_server(
    validator_api_port: int, context: Context
) -> JSONHTTPServer[Context]:
    # NOTE: `mypy` claims the handlers are not typed correctly although it does determine
    # the async callable to be a subtype of the declared type so it seems like a bug
    # and we will ignore for now...
    # See https://mypy.readthedocs.io/en/stable/more_types.html#typing-async-await
    return JSONHTTPServer(
        ValidatorAPIHandlers, context, validator_api_port  # type: ignore
    )


# def _derive_local_node_key(_key: PrivateKey, orchestration_profile: str) -> PrivateKey:
#     return PrivateKey(orchestration_profile[:1].encode() * 32)


# def _derive_port(maddr: Multiaddr, orchestration_profile: str) -> Multiaddr:
#     offset = ord(orchestration_profile[:1]) - ord("a")
#     port = int(maddr.value_for_protocol("tcp")) + offset
#     return Multiaddr.join(f"/ip4/{maddr.value_for_protocol('ip4')}", f"/tcp/{port}")


# def _derive_api_port(port: int, orchestration_profile: str) -> int:
#     offset = ord(orchestration_profile[:1]) - ord("a")
#     return port + offset


@dataclass
class SyncRequest:
    peer_id: PeerID
    start_slot: Slot
    count: int

    def get_batches(self, max_size: int) -> Iterable["SyncRequest"]:
        last_slot = self.start_slot + self.count
        batch_offsets = range(self.start_slot, self.start_slot + self.count, max_size)
        for offset in batch_offsets:
            remainder = last_slot - offset
            batch_size = max_size if remainder > max_size else remainder
            yield self.__class__(self.peer_id, Slot(offset), batch_size)


class BeaconNode:
    logger = logging.getLogger("trinity.nodes.beacon.full.BeaconNode")

    def __init__(
        self,
        local_node_key: PrivateKey,
        eth2_config: Eth2Config,
        clock: Clock,
        chain: BaseBeaconChain,
        validator_api_port: int,
        client_identifier: str,
        p2p_maddr: Multiaddr,
        preferred_nodes: Collection[Multiaddr],
        bootstrap_nodes: Collection[Multiaddr],
    ) -> None:
        self._local_key_pair = create_new_key_pair(local_node_key.to_bytes())
        self._eth2_config = eth2_config

        self._clock = clock
        self._chain = chain

        self._block_pool: Set[SignedBeaconBlock] = set()
        self._slashable_block_pool: Set[SignedBeaconBlock] = set()

        # FIXME: can we provide `p2p_maddr` as a default listening interface for `_mk_host`?
        peer_id = PeerID.from_pubkey(self._local_key_pair.public_key)
        if "p2p" in p2p_maddr:
            existing_peer_id = p2p_maddr.value_for_protocol("p2p")
            existing_p2p_maddr = Multiaddr(f"/p2p/{existing_peer_id}")
            self.logger.warning(
                "peer identity derived from local key pair %s overriding given identity %s",
                peer_id,
                existing_peer_id,
            )
            p2p_maddr = p2p_maddr.decapsulate(existing_p2p_maddr)
        self._p2p_maddr = p2p_maddr.encapsulate(Multiaddr(f"/p2p/{peer_id}"))

        # TODO: persist metadata and handle updates...
        self._metadata_provider = lambda: MetaData.create()
        self._peer_updater, self._peer_updates = trio.open_memory_channel[
            Tuple[PeerID, Any]
        ](0)
        self._host = Host(
            self._local_key_pair,
            peer_id,
            self._accept_peer_updates,
            self._get_status,
            self._get_finalized_root_by_epoch,
            self._get_block_by_slot,
            self._get_block_by_root,
            self._metadata_provider,
            self._get_fork_digest,
            self._eth2_config,
        )
        self._preferred_nodes = preferred_nodes
        self._bootstrap_nodes = bootstrap_nodes

        self._sync_notifier, self._sync_requests = trio.open_memory_channel[
            SyncRequest
        ](0)
        self._syncer = _mk_syncer()

        api_context = Context(
            client_identifier,
            eth2_config,
            self._syncer,
            self._chain,
            self._clock,
            _mk_block_broadcaster(self),
        )
        self.validator_api_port = validator_api_port
        self._validator_api_server = _mk_validator_api_server(
            self.validator_api_port, api_context
        )

    @classmethod
    def from_config(
        cls, config: BeaconNodeConfig, time_provider: TimeProvider = get_unix_time
    ) -> "BeaconNode":
        base_db = LevelDB(db_path=config.database_dir)
        chain_db = config.chain_db_class(base_db)
        recent_state = _resolve_recent_state(cls.logger, chain_db, config)

        if recent_state.slot == GENESIS_SLOT:
            chain_db.register_genesis(recent_state, SignedBeaconBlock)

        chain = config.chain_class.from_recent_state(chain_db, recent_state)

        genesis_time = recent_state.genesis_time
        clock = _mk_clock(config.eth2_config, genesis_time, time_provider)

        return cls(
            config.local_node_key,
            config.eth2_config,
            clock,
            chain,
            config.validator_api_port,
            config.client_identifier,
            config.p2p_maddr,
            config.preferred_nodes,
            config.bootstrap_nodes,
        )

    @property
    def current_tick(self) -> Tick:
        return self._clock.compute_current_tick()

    def _get_fork_digest(self) -> ForkDigest:
        # TODO: handle updates of fork digest
        state = self._chain.get_canonical_head_state()
        return compute_fork_digest(
            state.fork.current_version, state.genesis_validators_root
        )

    def _get_block_by_slot(self, slot: Slot) -> Optional[SignedBeaconBlock]:
        return self._chain.get_block_by_slot(slot)

    def _get_block_by_root(self, root: Root) -> Optional[SignedBeaconBlock]:
        try:
            block = self._chain.db.get_block_by_root(root, BeaconBlock)
            signature = self._chain.db.get_block_signature_by_root(block.hash_tree_root)
            return SignedBeaconBlock.create(message=block, signature=signature)
        except BlockNotFound:
            return None

    def _get_finalized_root_by_epoch(self, epoch: Epoch) -> Optional[Root]:
        """
        Return the (finalized) checkpoint root in the given ``epoch``.
        """
        slots_per_epoch = self._eth2_config.SLOTS_PER_EPOCH
        head_state = self._chain.get_canonical_head_state()
        finalized_checkpoint = head_state.finalized_checkpoint

        if epoch > finalized_checkpoint.epoch:
            return None

        if epoch == finalized_checkpoint.epoch:
            return finalized_checkpoint.root

        # NOTE: get a historical finalized root
        # This root will be the block at the start slot of the ``epoch``
        # in our canonical chain as implied by having a more recent
        # finalized head.
        slot = compute_start_slot_at_epoch(epoch, slots_per_epoch)
        block = self._get_block_by_slot(slot)
        return block.message.hash_tree_root

    def _get_status(self) -> Status:
        head_state = self._chain.get_canonical_head_state()
        head = self._chain.get_canonical_head()
        return Status.create(
            fork_digest=self._get_fork_digest(),
            finalized_root=head_state.finalized_checkpoint.root,
            finalized_epoch=head_state.finalized_checkpoint.epoch,
            head_root=head.hash_tree_root,
            head_slot=head.slot,
        )

    def on_block(self, block: SignedBeaconBlock) -> bool:
        """
        Return value indicates if block was successfully imported.
        """
        try:
            self._chain.on_block(block)
            self._on_block_imported(block)
            return True
        except ParentNotFoundError as exc:
            self._on_orphan_block(block, exc)
        except SlashableBlockError as exc:
            self._on_slashable_block(block, exc)
        except Exception as exc:
            self._on_block_failure(block, exc)
        return False

    def _try_import_orphan(self, imported_parent_root: Root) -> None:
        for orphan in self._block_pool:
            if orphan.message.parent_root == imported_parent_root:
                self._block_pool.discard(orphan)
                imported = self.on_block(orphan)
                if not imported:
                    self.logger.warning(
                        "failed to import orphan with root %s",
                        humanize_hash(orphan.hash_tree_root),
                    )
                    return

    def _on_block_imported(self, block: SignedBeaconBlock) -> None:
        self.logger.debug(
            "successfully imported signed block at slot %d with root %s",
            block.slot,
            humanize_hash(block.hash_tree_root),
        )
        self._try_import_orphan(block.message.hash_tree_root)
        # TODO: synchronize any operations from pools that are now on-chain

    def _on_block_failure(self, block: SignedBeaconBlock, exc: Exception) -> None:
        self.logger.exception(
            "failed to import block with root %s: %s",
            humanize_hash(block.hash_tree_root),
            exc,
        )
        # TODO: do not drop block?

    def _on_orphan_block(
        self, block: SignedBeaconBlock, _exc: ParentNotFoundError
    ) -> None:
        self.logger.debug(
            "failed to import block with root %s: missing parent with root %s",
            humanize_hash(block.hash_tree_root),
            humanize_hash(block.parent_root),
        )
        self._block_pool.add(block)

    def _on_slashable_block(
        self, block: SignedBeaconBlock, exc: SlashableBlockError
    ) -> None:
        self.logger.warning(
            "failed to import block with root %s: %s",
            humanize_hash(block.hash_tree_root),
            exc,
        )
        # NOTE: chain will write the block in ``on_block`` but not the block's state
        # See the place that exception is raised for further rationale.
        # TODO: pipe to "slasher" software...
        self._slashable_block_pool.add(block)

    async def broadcast_block(self, block: SignedBeaconBlock) -> None:
        await self._host.broadcast_block(block)

    async def _iterate_clock(
        self, task_status: TaskStatus[None] = trio.TASK_STATUS_IGNORED
    ) -> None:
        task_status.started()
        async for tick in self._clock:
            if tick.count == 0:
                self.logger.debug(
                    "slot %d [number %d in epoch %d]",
                    tick.slot,
                    tick.slot_in_epoch(self._eth2_config.SLOTS_PER_EPOCH),
                    tick.epoch,
                )
            self._chain.on_tick(tick)

    async def _run_validator_api(
        self, task_status: TaskStatus[None] = trio.TASK_STATUS_IGNORED
    ) -> None:
        server = self._validator_api_server
        async with trio.open_nursery() as nursery:
            self.validator_api_port = await nursery.start(server.serve)
            self.logger.info(
                "validator HTTP API server listening on %d", self.validator_api_port
            )
            task_status.started()

    async def _accept_peer_updates(self, peer_id: PeerID, update: Any) -> None:
        await self._peer_updater.send((peer_id, update))

    async def _run_host(
        self, task_status: TaskStatus[None] = trio.TASK_STATUS_IGNORED
    ) -> None:
        host = self._host
        listen_maddr = self._p2p_maddr
        try:
            # NOTE the following code relies on knowledge of the internals here...
            # We ideally want better encapsulation but it looks like it will
            # involve separating out the ``Service`` abstraction from the
            # various ``libp2p`` abstractions...
            async with host.run(listen_addrs=(listen_maddr,)):
                task_status.started()
                self.logger.info("peer listening at %s", listen_maddr)
                async with background_trio_service(host._gossiper.pubsub):
                    async with background_trio_service(host._gossiper.gossipsub):
                        await self._connect_preferred_nodes()
                        await self._connect_bootstrap_nodes()

                        # NOTE: need to connect *some* peers first before
                        # subscribing to gossip
                        # FIXME: can likely move this inside ``host``.
                        await host.subscribe_gossip_channels()

                        await self._handle_gossip()

                        await host.unsubscribe_gossip_channels()
        except Exception as e:
            # TODO: likely want to catch exceptions in a more granular way
            # and restart the host...
            self.logger.exception(e)

    async def _connect_preferred_nodes(self) -> None:
        async with trio.open_nursery() as nursery:
            for preferred_maddr in self._preferred_nodes:
                nursery.start_soon(self._host.add_peer_from_maddr, preferred_maddr)

    async def _connect_bootstrap_nodes(self) -> None:
        async with trio.open_nursery() as nursery:
            for bootstrap_maddr in self._bootstrap_nodes:
                nursery.start_soon(self._host.add_peer_from_maddr, bootstrap_maddr)

    async def _handle_gossip(self) -> None:
        gossip_handlers = (self._handle_block_gossip,)
        async with trio.open_nursery() as nursery:
            for handler in gossip_handlers:
                nursery.start_soon(handler)

    async def _handle_block_gossip(self) -> None:
        async for block in self._host.stream_block_gossip():
            self.on_block(block)

    def _determine_sync_request(
        self, peer_id: PeerID, status: Status
    ) -> Optional[SyncRequest]:
        """
        If the peer has a higher finalized epoch or head slot, sync blocks from them.
        """
        head_state = self._chain.get_canonical_head_state()
        finalized_slot = compute_start_slot_at_epoch(
            head_state.finalized_checkpoint.epoch, self._eth2_config.SLOTS_PER_EPOCH
        )
        if finalized_slot < status.head_slot:
            span = status.head_slot - finalized_slot
            return SyncRequest(peer_id, Slot(finalized_slot + 1), span)
        else:
            return None

    async def _manage_peers(
        self, task_status: TaskStatus[None] = trio.TASK_STATUS_IGNORED
    ) -> None:
        task_status.started()
        async with self._peer_updates:
            async for peer_id, update in self._peer_updates:
                if isinstance(update, Status):
                    request = self._determine_sync_request(peer_id, update)
                    if request:
                        await self._sync_notifier.send(request)
                elif isinstance(update, GoodbyeReason):
                    self.logger.debug(
                        "recv'd goodbye from %s with reason: %s", peer_id, update
                    )
                    await self._host.drop_peer(peer_id)
                elif isinstance(update, MetaDataSeqNumber):
                    # TODO: track peers and their metadata
                    self.logger.debug(
                        "recv'd ping from %s with seq number: %s", peer_id, update
                    )

    async def _sync_batch(
        self, batch: SyncRequest, start_time: float, start_slot: Slot
    ) -> bool:
        try:
            count = 0
            async for block in self._host.get_blocks_by_range(
                batch.peer_id, batch.start_slot, batch.count
            ):
                count += 1
                imported = self.on_block(block)
                if not imported:
                    self.logger.warning(
                        "an issue with sync of this block %s",
                        humanize_hash(block.hash_tree_root),
                    )
                    return False
                if count % 8 == 0:
                    now = time.time()
                    slots = block.slot - start_slot
                    slots_per_second = slots / (now - start_time)
                    self.logger.info(
                        "synced to slot %d, syncing at [ %2f slots/sec ]",
                        block.slot,
                        slots_per_second,
                    )
        except Exception as e:
            self.logger.exception(e)
            return False
        return True

    async def _manage_sync_requests(
        self, task_status: TaskStatus[None] = trio.TASK_STATUS_IGNORED
    ) -> None:
        task_status.started()
        async with self._sync_requests:
            async for request in self._sync_requests:
                start_time = time.time()
                start_slot = request.start_slot
                self.logger.info(
                    "starting sync of %d slots at %s", request.count, time.ctime()
                )

                # NOTE: seeing more robust operation with lower batch size...
                for batch in request.get_batches(128):
                    success = await self._sync_batch(batch, start_time, start_slot)
                    for _ in range(SYNC_REATTEMPT_COUNT):
                        if not success:
                            success = await self._sync_batch(
                                batch, start_time, start_slot
                            )
                        else:
                            break

    async def run(
        self, task_status: TaskStatus[None] = trio.TASK_STATUS_IGNORED
    ) -> None:
        tasks = (
            self._iterate_clock,
            self._run_validator_api,
            self._run_host,
            self._manage_peers,
            self._manage_sync_requests,
        )

        async with trio.open_nursery() as nursery:
            for task in tasks:
                await nursery.start(task)
            task_status.started()


def _resolve_recent_state(
    logger: logging.Logger, chain_db: BaseBeaconChainDB, config: BeaconNodeConfig
) -> BeaconState:
    """
    Find a recent ``BeaconState`` either supplied by the user or stored in the database.

    NOTE: ``chain_db`` may or may not have a recent state, depending on the lifecycle of this node.
    """
    last_recorded_state = chain_db.get_weak_subjectivity_state()
    if config.recent_state_ssz:
        with open(config.recent_state_ssz, "rb") as recent_state_file:
            supplied_state = ssz.decode(recent_state_file.read(), BeaconState)
        if last_recorded_state is None:
            logger.info(
                "using supplied weak subjectivity state with root %s",
                humanize_hash(supplied_state.hash_tree_root),
            )
            # TODO ensure genesis state has been supplied, at some point.
            chain_db.persist_state(supplied_state)
            chain_db.persist_weak_subjectivity_state_root(supplied_state.hash_tree_root)
        else:
            if supplied_state.slot >= last_recorded_state.slot:
                logger.info(
                    "using supplied weak subjectivity state with root %s"
                    " as it is newer than existing state in DB with root %s",
                    humanize_hash(supplied_state.hash_tree_root),
                    humanize_hash(last_recorded_state.hash_tree_root),
                )
                chain_db.persist_state(supplied_state)
                chain_db.persist_weak_subjectivity_state_root(
                    supplied_state.hash_tree_root
                )
            else:
                logger.info(
                    "using last weak subjectivity state in DB with root %s"
                    "as it is newer than the supplied state with root %s",
                    humanize_hash(last_recorded_state.hash_tree_root),
                    humanize_hash(supplied_state.hash_tree_root),
                )
                return last_recorded_state
    else:
        if last_recorded_state is None:
            raise Exception(
                "could not find a recent state to use for weak subjectivity... please supply one to begin."
            )
        else:
            logger.info(
                "using last weak subjectivity state in DB with root %s",
                humanize_hash(last_recorded_state.hash_tree_root),
            )
            return last_recorded_state


def _mk_block_broadcaster(node: BeaconNode) -> BlockBroadcasterAPI:
    class _broadcast(BlockBroadcasterAPI):
        async def broadcast_block(self, block: SignedBeaconBlock) -> None:
            # TODO / NOTE: in the future, pipe into external facing block import
            # to collect same validations as blocks from other sources, not just validator client
            imported = node.on_block(block)
            if imported:
                await node.broadcast_block(block)

    return _broadcast()
