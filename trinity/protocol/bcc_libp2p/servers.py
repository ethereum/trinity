import asyncio
from typing import (
    Awaitable,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)

from cancel_token import (
    CancelToken,
)
from eth.exceptions import (
    BlockNotFound,
)
from eth_utils import (
    ValidationError,
    encode_hex,
    to_tuple,
)

import ssz

from libp2p.pubsub.pb import rpc_pb2

from p2p.service import BaseService

from eth2.beacon.chains.base import (
    BaseBeaconChain,
)
from eth2.beacon.operations.attestation_pool import AttestationPool
from eth2.beacon.types.aggregate_and_proof import AggregateAndProof
from eth2.beacon.types.attestations import (
    Attestation,
)
from eth2.beacon.types.blocks import (
    BaseSignedBeaconBlock,
    SignedBeaconBlock,
)
from eth2.beacon.typing import (
    Root,
    SubnetId,
)
from eth2.beacon.typing import CommitteeIndex, Slot

from trinity.protocol.bcc_libp2p.node import Node

from .configs import (
    PUBSUB_TOPIC_BEACON_AGGREGATE_AND_PROOF,
    PUBSUB_TOPIC_BEACON_BLOCK,
    PUBSUB_TOPIC_BEACON_ATTESTATION,
    PUBSUB_TOPIC_COMMITTEE_BEACON_ATTESTATION,
)

PROCESS_ORPHAN_BLOCKS_PERIOD = 10.0


class OrphanBlockPool:
    """
    Store the orphan blocks(the blocks who arrive before their parents).
    """
    # TODO: can probably use lru-cache or even database
    _pool: Set[BaseSignedBeaconBlock]

    def __init__(self) -> None:
        self._pool = set()

    def __len__(self) -> int:
        return len(self._pool)

    def __contains__(self, block_or_block_root: Union[BaseSignedBeaconBlock, Root]) -> bool:
        block_root: Root
        if isinstance(block_or_block_root, BaseSignedBeaconBlock):
            block_root = block_or_block_root.message.hash_tree_root
        elif isinstance(block_or_block_root, bytes):
            block_root = block_or_block_root
        else:
            raise TypeError("`block_or_block_root` should be `BaseSignedBeaconBlock` or `Root`")
        try:
            self.get(block_root)
            return True
        except BlockNotFound:
            return False

    def to_list(self) -> List[BaseSignedBeaconBlock]:
        return list(self._pool)

    def get(self, block_root: Root) -> BaseSignedBeaconBlock:
        for block in self._pool:
            if block.message.hash_tree_root == block_root:
                return block
        raise BlockNotFound(f"No block with message.hash_tree_root {block_root.hex()} is found")

    def add(self, block: BaseSignedBeaconBlock) -> None:
        if block in self._pool:
            return
        self._pool.add(block)

    def pop_children(self, block_root: Root) -> Tuple[BaseSignedBeaconBlock, ...]:
        children = tuple(
            orphan_block
            for orphan_block in self._pool
            if orphan_block.parent_root == block_root
        )
        self._pool.difference_update(children)
        return children


class BCCReceiveServer(BaseService):

    chain: BaseBeaconChain
    p2p_node: Node
    topic_msg_queues: Dict[str, 'asyncio.Queue[rpc_pb2.Message]']
    unaggregated_attestation_pool: AttestationPool
    aggregated_attestation_pool: AttestationPool
    orphan_block_pool: OrphanBlockPool
    subnets: Set[SubnetId]

    def __init__(
            self,
            chain: BaseBeaconChain,
            p2p_node: Node,
            topic_msg_queues: Dict[str, 'asyncio.Queue[rpc_pb2.Message]'],
            subnets: Optional[Set[SubnetId]] = None,
            cancel_token: CancelToken = None) -> None:
        super().__init__(cancel_token)
        self.chain = chain
        self.p2p_node = p2p_node
        self.topic_msg_queues = topic_msg_queues
        self.subnets = subnets if subnets is not None else set()
        self.unaggregated_attestation_pool = AttestationPool()
        self.aggregated_attestation_pool = AttestationPool()
        self.orphan_block_pool = OrphanBlockPool()
        self.ready = asyncio.Event()

    async def _run(self) -> None:
        while not self.p2p_node.is_started:
            await self.sleep(0.5)
        self.logger.info("BCCReceiveServer up")

        # Handle gossipsub messages
        self.run_daemon_task(self._handle_beacon_attestation_loop())
        self.run_daemon_task(self._handle_beacon_block_loop())
        self.run_daemon_task(self._handle_aggregate_and_proof_loop())
        self.run_daemon_task(self._handle_committee_beacon_attestation_loop())
        self.run_daemon_task(self._process_orphan_blocks_loop())
        self.ready.set()
        await self.cancellation()

    #
    # Daemon tasks
    #
    async def _handle_message(
            self,
            topic: str,
            handler: Callable[[rpc_pb2.Message], Awaitable[None]]) -> None:
        queue = self.topic_msg_queues[topic]
        while True:
            message = await queue.get()
            # Libp2p let the sender receive their own message, which we need to ignore here.
            if message.from_id == self.p2p_node.peer_id:
                queue.task_done()
                continue
            else:
                await handler(message)
                queue.task_done()

    async def _handle_beacon_attestation_loop(self) -> None:
        await self._handle_message(
            PUBSUB_TOPIC_BEACON_ATTESTATION,
            self._handle_beacon_attestation
        )

    async def _handle_committee_beacon_attestation_loop(self) -> None:
        while True:
            await asyncio.sleep(0.5)
            for subnet_id in self.subnets:
                topic = PUBSUB_TOPIC_COMMITTEE_BEACON_ATTESTATION.substitute(
                    subnet_id=str(subnet_id)
                )
                try:
                    queue = self.topic_msg_queues[topic]
                    message = queue.get_nowait()
                except asyncio.QueueEmpty:
                    continue
                else:
                    await self._handle_committee_beacon_attestation(message)

    async def _handle_aggregate_and_proof_loop(self) -> None:
        await self._handle_message(
            PUBSUB_TOPIC_BEACON_AGGREGATE_AND_PROOF,
            self._handle_beacon_aggregate_and_proof,
        )

    async def _handle_beacon_block_loop(self) -> None:
        await self._handle_message(
            PUBSUB_TOPIC_BEACON_BLOCK,
            self._handle_beacon_block
        )

    async def _process_orphan_blocks_loop(self) -> None:
        """
        Periodically requesting for parent blocks of the
        orphan blocks in the orphan block pool.
        """
        while True:
            await self.sleep(PROCESS_ORPHAN_BLOCKS_PERIOD)
            if len(self.orphan_block_pool) == 0:
                continue
            # TODO: Prune Bruce Wayne type of orphan block
            # (whose parent block seemingly never going to show up)
            orphan_blocks = self.orphan_block_pool.to_list()
            parent_roots = set(block.parent_root for block in orphan_blocks)
            block_roots = set(block.message.hash_tree_root for block in orphan_blocks)
            # Remove dependent orphan blocks
            parent_roots.difference_update(block_roots)
            # Keep requesting parent blocks from all peers
            for peer in self.p2p_node.handshaked_peers.peers.values():
                if len(parent_roots) == 0:
                    break
                blocks = await peer.request_beacon_blocks_by_root(
                    tuple(parent_roots)
                )
                for block in blocks:
                    try:
                        parent_roots.remove(block.message.hash_tree_root)
                    except ValueError:
                        self.logger.debug(
                            "peer=%s sent incorrect block=%s",
                            peer._id,
                            encode_hex(block.message.hash_tree_root),
                        )
                        # This should not happen if peers are returning correct blocks
                        continue
                    else:
                        self._process_received_block(block)

    #
    # Message handlers
    #
    async def _handle_committee_beacon_attestation(self, msg: rpc_pb2.Message) -> None:
        await self._handle_beacon_attestation(msg)

    async def _handle_beacon_aggregate_and_proof(self, msg: rpc_pb2.Message) -> None:
        aggregate_and_proof = ssz.decode(msg.data, sedes=AggregateAndProof)

        self.logger.debug("Received aggregate_and_proof=%s", aggregate_and_proof)

        self._add_attestation(self.aggregated_attestation_pool, aggregate_and_proof.aggregate)

    async def _handle_beacon_attestation(self, msg: rpc_pb2.Message) -> None:
        attestation = ssz.decode(msg.data, sedes=Attestation)

        self.logger.debug("Received attestation=%s", attestation)

        self._add_attestation(self.unaggregated_attestation_pool, attestation)

    async def _handle_beacon_block(self, msg: rpc_pb2.Message) -> None:
        block = ssz.decode(msg.data, SignedBeaconBlock)
        self._process_received_block(block)

    def _is_attestation_new(
        self, attestation_pool: AttestationPool, attestation: Attestation
    ) -> bool:
        """
        Check if the attestation is already in the database or the attestion pool.
        """
        if attestation.hash_tree_root in attestation_pool:
            return False
        return not self.chain.attestation_exists(attestation.hash_tree_root)

    def _add_attestation(self, attestation_pool: AttestationPool, attestation: Attestation) -> None:
        # Check if attestation has been seen already.
        if not self._is_attestation_new(attestation_pool, attestation):
            return

        # Add new attestation to attestation pool.
        attestation_pool.add(attestation)

    def _process_received_block(self, block: BaseSignedBeaconBlock) -> None:
        # If the block is an orphan, put it to the orphan pool
        self.logger.debug(
            'Received block over gossip. slot=%d message.hash_tree_root=%s',
            block.slot,
            block.message.hash_tree_root.hex(),
        )
        if not self._is_block_root_in_db(block.parent_root):
            if block not in self.orphan_block_pool:
                self.logger.debug("Found orphan_block=%s", block)
                self.orphan_block_pool.add(block)
            return
        try:
            self.chain.import_block(block)
            self.logger.info(
                "Successfully imported block=%s",
                encode_hex(block.message.hash_tree_root),
            )
        # If the block is invalid, we should drop it.
        except ValidationError as error:
            # TODO: Possibly drop all of its descendants in `self.orphan_block_pool`?
            self.logger.debug("Fail to import block=%s  reason=%s", block, error)
        else:
            # Successfully imported the block. See if any blocks in `self.orphan_block_pool`
            # depend on it. If there are, try to import them.
            # TODO: should be done asynchronously?
            self._try_import_orphan_blocks(block.message.hash_tree_root)
            # Remove attestations in block that are also in the attestation pool.
            self.unaggregated_attestation_pool.batch_remove(block.body.attestations)

    def _try_import_orphan_blocks(self, parent_root: Root) -> None:
        """
        Perform ``chain.import`` on the blocks in ``self.orphan_block_pool`` in breadth-first
        order, starting from the children of ``parent_root``.
        """
        imported_roots: List[Root] = []

        imported_roots.append(parent_root)
        while len(imported_roots) != 0:
            current_parent_root = imported_roots.pop()
            # Only process the children if the `current_parent_root` is already in db.
            if not self._is_block_root_in_db(block_root=current_parent_root):
                continue
            # If succeeded, handle the orphan blocks which depend on this block.
            children = self.orphan_block_pool.pop_children(current_parent_root)
            if len(children) > 0:
                self.logger.debug(
                    "Blocks=%s match their parent block, parent_root=%s",
                    children,
                    encode_hex(current_parent_root),
                )
            for block in children:
                try:
                    self.chain.import_block(block)
                    self.logger.info(
                        "Successfully imported block=%s",
                        encode_hex(block.message.hash_tree_root),
                    )
                    imported_roots.append(block.message.hash_tree_root)
                except ValidationError as error:
                    # TODO: Possibly drop all of its descendants in `self.orphan_block_pool`?
                    self.logger.debug("Fail to import block=%s  reason=%s", block, error)

    def _is_block_root_in_orphan_block_pool(self, block_root: Root) -> bool:
        return block_root in self.orphan_block_pool

    def _is_block_root_in_db(self, block_root: Root) -> bool:
        try:
            self.chain.get_block_by_root(block_root=block_root)
            return True
        except BlockNotFound:
            return False

    def _is_block_root_seen(self, block_root: Root) -> bool:
        if self._is_block_root_in_orphan_block_pool(block_root=block_root):
            return True
        return self._is_block_root_in_db(block_root=block_root)

    def _is_block_seen(self, block: BaseSignedBeaconBlock) -> bool:
        return self._is_block_root_seen(block_root=block.message.hash_tree_root)

    #
    # Exposed APIs for Validator
    #
    @to_tuple
    def get_ready_attestations(
        self, current_slot: Slot, is_aggregated: bool
    ) -> Iterable[Attestation]:
        """
        Get the attestations that are ready to be included in ``current_slot`` block.
        """
        config = self.chain.get_state_machine().config

        if is_aggregated:
            attestation_pool = self.aggregated_attestation_pool
        else:
            attestation_pool = self.unaggregated_attestation_pool

        return attestation_pool.get_valid_attestation_by_current_slot(
            current_slot,
            config,
        )

    def get_aggregatable_attestations(
        self,
        slot: Slot,
        committee_index: CommitteeIndex
    ) -> Tuple[Attestation, ...]:
        """
        Get the attestations of ``slot`` and ``committee_index``.
        """
        try:
            block = self.chain.get_canonical_block_by_slot(slot)
        except BlockNotFound:
            return ()

        beacon_block_root = block.message.hash_tree_root
        return self.unaggregated_attestation_pool.get_acceptable_attestations(
            slot, committee_index, beacon_block_root
        )

    def import_attestation(self, attestation: Attestation, is_aggregated: bool) -> None:
        if is_aggregated:
            self.aggregated_attestation_pool.add(attestation)
        else:
            self.unaggregated_attestation_pool.add(attestation)
