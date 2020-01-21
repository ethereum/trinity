from eth2.beacon.tools.factories import BeaconChainFactory
import asyncio
from typing import Any, AsyncIterator, Dict, Iterable, Collection, Tuple, Type, Sequence

from async_generator import asynccontextmanager

from cancel_token import CancelToken

from lahja import EndpointAPI
from libp2p.crypto.secp256k1 import create_new_key_pair
from libp2p.peer.id import ID
from libp2p.peer.peerinfo import PeerInfo

from eth_utils import to_tuple

from eth.constants import ZERO_HASH32

from p2p.service import run_service
from p2p.tools.factories import get_open_port, CancelTokenFactory

from eth2.beacon.constants import EMPTY_SIGNATURE, ZERO_ROOT
from eth2.beacon.fork_choice.higher_slot import HigherSlotScoring
from eth2.beacon.types.states import BeaconState
from eth2.beacon.types.blocks import (
    BeaconBlock,
    BeaconBlockBody,
    BaseSignedBeaconBlock,
    SignedBeaconBlock,
)
from eth2.beacon.state_machines.forks.serenity import SERENITY_CONFIG
from eth2.beacon.typing import Slot
from eth2.configs import Eth2GenesisConfig
from multiaddr import Multiaddr

from trinity.db.beacon.chain import AsyncBeaconChainDB

from trinity.protocol.bcc_libp2p.node import Node, PeerPool, Peer
from trinity.protocol.bcc_libp2p.servers import BCCReceiveServer
from trinity.sync.beacon.chain import BeaconChainSyncer

from .factories import AtomicDBFactory

try:
    import factory
except ImportError:
    raise ImportError(
        "The p2p.tools.factories module requires the `factory_boy` library."
    )


SERENITY_GENESIS_CONFIG = Eth2GenesisConfig(SERENITY_CONFIG)


#
# LibP2P
#


class NodeFactory(factory.Factory):
    class Meta:
        model = Node

    key_pair = factory.LazyFunction(create_new_key_pair)
    listen_ip = "127.0.0.1"
    listen_port = factory.LazyFunction(get_open_port)
    security_protocol_ops = None
    muxer_protocol_ops = None
    gossipsub_params = None
    cancel_token = None
    bootstrap_nodes = None
    preferred_nodes: Tuple[Multiaddr, ...] = tuple()
    subnets: None
    chain = factory.SubFactory(BeaconChainFactory)
    event_bus = None

    @classmethod
    def create_batch(cls, number: int) -> Tuple[Node, ...]:
        return tuple(cls() for _ in range(number))


class PeerFactory(factory.Factory):
    class Meta:
        model = Peer

    _id = factory.Sequence(lambda n: ID(f"peer{n}"))
    node = factory.SubFactory(NodeFactory)
    head_fork_version = None
    finalized_root = ZERO_HASH32
    finalized_epoch = 0
    head_root = ZERO_HASH32
    head_slot = 0


@asynccontextmanager
async def ConnectionPairFactory(
    alice_chaindb: AsyncBeaconChainDB = None,
    alice_branch: Collection[BaseSignedBeaconBlock] = None,
    bob_chaindb: AsyncBeaconChainDB = None,
    bob_branch: Collection[BaseSignedBeaconBlock] = None,
    genesis_state: BeaconState = None,
    alice_event_bus: EndpointAPI = None,
    cancel_token: CancelToken = None,
    handshake: bool = True,
) -> AsyncIterator[Tuple[Node, Node]]:
    if cancel_token is None:
        cancel_token = CancelTokenFactory()
    alice_kwargs: Dict[str, Any] = {}
    bob_kwargs: Dict[str, Any] = {}

    if alice_chaindb is not None:
        alice_kwargs["chain__db"] = alice_chaindb.db
    if alice_branch is not None:
        alice_kwargs["chain__branch"] = alice_branch
    if bob_chaindb is not None:
        bob_kwargs["chain__db"] = bob_chaindb.db
    if bob_branch is not None:
        bob_kwargs["chain__branch"] = bob_branch
    if genesis_state is not None:
        alice_kwargs["chain__genesis_state"] = genesis_state
        bob_kwargs["chain__genesis_state"] = genesis_state

    alice = NodeFactory(
        cancel_token=cancel_token, event_bus=alice_event_bus, **alice_kwargs
    )
    bob = NodeFactory(cancel_token=cancel_token, **bob_kwargs)
    async with run_service(alice), run_service(bob):
        await asyncio.sleep(0.01)
        await alice.host.connect(
            PeerInfo(peer_id=bob.peer_id, addrs=[bob.listen_maddr])
        )
        await asyncio.sleep(0.01)
        if handshake:
            await alice.request_status(bob.peer_id)
            await asyncio.sleep(0.01)
        yield alice, bob


class BeaconBlockBodyFactory(factory.Factory):
    class Meta:
        model = BeaconBlockBody.create


class BeaconBlockFactory(factory.Factory):
    class Meta:
        model = BeaconBlock.create

    slot = SERENITY_GENESIS_CONFIG.GENESIS_SLOT
    parent_root = ZERO_ROOT
    state_root = ZERO_HASH32
    body = factory.SubFactory(BeaconBlockBodyFactory)


class SignedBeaconBlockFactory(factory.Factory):
    class Meta:
        model = SignedBeaconBlock.create

    message = factory.SubFactory(BeaconBlockFactory)
    signature = EMPTY_SIGNATURE

    @classmethod
    def _create(
        cls, model_class: Type[BaseSignedBeaconBlock], *args: Any, **kwargs: Any
    ) -> BaseSignedBeaconBlock:
        print(kwargs)
        parent = kwargs.pop("parent", None)
        if parent is not None:
            return (
                super()
                ._create(model_class, *args, **kwargs)
                .transform(("message", "parent_root"), parent.message.hash_tree_root)
                .transform(("message", "slot"), parent.message.slot + 1)
            )
        return super()._create(model_class, *args, **kwargs)

    @classmethod
    @to_tuple
    def create_branch(
        cls, length: int, root: BaseSignedBeaconBlock = None, **kwargs: Any
    ) -> Iterable[BaseSignedBeaconBlock]:
        if length == 0:
            return

        if root is None:
            root = cls()

        parent = cls(parent=root, **kwargs)
        yield parent

        for _ in range(length - 1):
            child = cls(parent=parent)
            yield child
            parent = child

    @classmethod
    @to_tuple
    def create_branch_by_slots(
        cls, slots: Sequence[Slot], root: BaseSignedBeaconBlock = None, **kwargs: Any
    ) -> Iterable[BaseSignedBeaconBlock]:
        if root is None:
            root = cls()

        parent = cls(
            message__parent_root=root.message.hash_tree_root, message__slot=slots[0], **kwargs
        )
        yield parent
        for slot in slots[1:]:
            child = cls(message__parent_root=parent.message.hash_tree_root, message__slot=slot)
            yield child
            parent = child


class AsyncBeaconChainDBFactory(factory.Factory):
    class Meta:
        model = AsyncBeaconChainDB

    db = factory.SubFactory(AtomicDBFactory)
    genesis_config = SERENITY_GENESIS_CONFIG


class ReceiveServerFactory(factory.Factory):
    class Meta:
        model = BCCReceiveServer

    chain = None
    p2p_node = factory.SubFactory(NodeFactory)
    topic_msg_queues = None
    subnets = None
    cancel_token = None

    @classmethod
    def create_batch(cls, number: int) -> Tuple[Node, ...]:
        return tuple(cls() for _ in range(number))


class SimpleWriterBlockImporter:
    """
    ``SimpleWriterBlockImporter`` just persists any imported blocks to the
    database provided at instantiation.
    """

    def __init__(self, chain_db: AsyncBeaconChainDB) -> None:
        self._chain_db = chain_db

    def import_block(
        self, block: BaseSignedBeaconBlock
    ) -> Tuple[
        BaseSignedBeaconBlock,
        Tuple[BaseSignedBeaconBlock, ...],
        Tuple[BaseSignedBeaconBlock, ...],
    ]:
        new_blocks, old_blocks = self._chain_db.persist_block(
            block, SignedBeaconBlock, HigherSlotScoring()
        )
        return None, new_blocks, old_blocks


class BeaconChainSyncerFactory(factory.Factory):
    class Meta:
        model = BeaconChainSyncer

    chain_db = factory.SubFactory(AsyncBeaconChainDBFactory)
    peer_pool = factory.LazyAttribute(lambda _: PeerPool())
    block_importer = factory.LazyAttribute(
        lambda obj: SimpleWriterBlockImporter(obj.chain_db)
    )
    genesis_config = SERENITY_GENESIS_CONFIG
    event_bus = None
    token = None
