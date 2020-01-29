from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Sequence, Type, TypeVar

from eth._utils.datatypes import Configurable
from eth.constants import ZERO_HASH32
from eth_typing import BLSSignature, Hash32
from eth_utils import humanize_hash
from ssz.hashable_container import HashableContainer
from ssz.sedes import List, bytes32, bytes96, uint64

from eth2.beacon.constants import EMPTY_SIGNATURE, GENESIS_PARENT_ROOT, ZERO_ROOT
from eth2.beacon.typing import FromBlockParams, Root, Slot

from .attestations import Attestation
from .attester_slashings import AttesterSlashing
from .block_headers import BeaconBlockHeader, SignedBeaconBlockHeader
from .defaults import default_slot, default_tuple
from .deposits import Deposit
from .eth1_data import Eth1Data, default_eth1_data
from .proposer_slashings import ProposerSlashing
from .voluntary_exits import SignedVoluntaryExit

if TYPE_CHECKING:
    from eth2.beacon.db.chain import BaseBeaconChainDB  # noqa: F401


TBeaconBlockBody = TypeVar("TBeaconBlockBody", bound="BeaconBlockBody")


class BeaconBlockBody(HashableContainer):

    fields = [
        ("randao_reveal", bytes96),
        ("eth1_data", Eth1Data),
        ("graffiti", bytes32),
        ("proposer_slashings", List(ProposerSlashing, 16)),
        ("attester_slashings", List(AttesterSlashing, 1)),
        ("attestations", List(Attestation, 128)),
        ("deposits", List(Deposit, 16)),
        ("voluntary_exits", List(SignedVoluntaryExit, 16)),
    ]

    @classmethod
    def create(
        cls: Type[TBeaconBlockBody],
        *,
        randao_reveal: bytes96 = EMPTY_SIGNATURE,
        eth1_data: Eth1Data = default_eth1_data,
        graffiti: Hash32 = ZERO_HASH32,
        proposer_slashings: Sequence[ProposerSlashing] = default_tuple,
        attester_slashings: Sequence[AttesterSlashing] = default_tuple,
        attestations: Sequence[Attestation] = default_tuple,
        deposits: Sequence[Deposit] = default_tuple,
        voluntary_exits: Sequence[SignedVoluntaryExit] = default_tuple,
    ) -> TBeaconBlockBody:
        return super().create(
            randao_reveal=randao_reveal,
            eth1_data=eth1_data,
            graffiti=graffiti,
            proposer_slashings=proposer_slashings,
            attester_slashings=attester_slashings,
            attestations=attestations,
            deposits=deposits,
            voluntary_exits=voluntary_exits,
        )

    @property
    def is_empty(self) -> bool:
        return self == BeaconBlockBody.create()

    def __str__(self) -> str:
        return (
            f"randao_reveal={humanize_hash(self.randao_reveal)},"
            f" graffiti={humanize_hash(self.graffiti)},"
            f" proposer_slashings={self.proposer_slashings},"
            f" attester_slashings={self.attester_slashings},"
            f" attestations={self.attestations},"
            f" deposits={self.deposits},"
            f" voluntary_exits={self.voluntary_exits},"
        )

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}: {str(self)}>"


default_beacon_block_body = BeaconBlockBody.create()


TBaseBeaconBlock = TypeVar("TBaseBeaconBlock", bound="BaseBeaconBlock")


class BaseBeaconBlock(HashableContainer, Configurable, ABC):
    fields = [
        ("slot", uint64),
        ("parent_root", bytes32),
        ("state_root", bytes32),
        ("body", BeaconBlockBody),
    ]

    @classmethod
    def create(
        cls: Type[TBaseBeaconBlock],
        *,
        slot: Slot = default_slot,
        parent_root: Root = ZERO_ROOT,
        state_root: Hash32 = ZERO_HASH32,
        body: BeaconBlockBody = default_beacon_block_body,
    ) -> TBaseBeaconBlock:
        return super().create(
            slot=slot, parent_root=parent_root, state_root=state_root, body=body
        )

    def __str__(self) -> str:
        return (
            f"[hash_tree_root]={humanize_hash(self.hash_tree_root)},"
            f" slot={self.slot},"
            f" parent_root={humanize_hash(self.parent_root)},"
            f" state_root={humanize_hash(self.state_root)},"
            f" body=({self.body})"
        )

    @property
    def is_genesis(self) -> bool:
        return self.parent_root == GENESIS_PARENT_ROOT

    @property
    def header(self) -> BeaconBlockHeader:
        return BeaconBlockHeader.create(
            slot=self.slot,
            parent_root=self.parent_root,
            state_root=self.state_root,
            body_root=self.body.hash_tree_root,
        )

    @classmethod
    @abstractmethod
    def from_root(cls, root: Root, chaindb: "BaseBeaconChainDB") -> "BaseBeaconBlock":
        """
        Return the block denoted by the given block root.
        """
        ...

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}: {str(self)}>"


TBeaconBlock = TypeVar("TBeaconBlock", bound="BeaconBlock")


class BeaconBlock(BaseBeaconBlock):
    block_body_class = BeaconBlockBody

    @classmethod
    def from_parent(
        cls: Type[TBeaconBlock],
        parent_block: "BaseBeaconBlock",
        block_params: FromBlockParams,
    ) -> TBeaconBlock:

        if block_params.slot is None:
            slot = parent_block.slot + 1
        else:
            slot = block_params.slot

        block = cls.create(
            slot=slot,
            parent_root=parent_block.hash_tree_root,
            state_root=parent_block.state_root,
            body=cls.block_body_class.create(),
        )
        return block

    @classmethod
    def from_root(cls, root: Root, chaindb: "BaseBeaconChainDB") -> "BeaconBlock":
        """
        Return the block denoted by the given block ``root``.
        """
        block = chaindb.get_block_by_root(root, cls)
        body = cls.block_body_class.create(
            randao_reveal=block.body.randao_reveal,
            eth1_data=block.body.eth1_data,
            graffiti=block.body.graffiti,
            proposer_slashings=block.body.proposer_slashings,
            attester_slashings=block.body.attester_slashings,
            attestations=block.body.attestations,
            deposits=block.body.deposits,
            voluntary_exits=block.body.voluntary_exits,
        )

        return cls.create(
            slot=block.slot,
            parent_root=block.parent_root,
            state_root=block.state_root,
            body=body,
        )

    @classmethod
    def from_header(
        cls: Type[TBaseBeaconBlock], header: BeaconBlockHeader
    ) -> TBeaconBlock:
        return cls.create(
            slot=header.slot,
            parent_root=header.parent_root,
            state_root=header.state_root,
            body=BeaconBlockBody(),
        )


default_beacon_block = BeaconBlock.create()


TSignedBeaconBlock = TypeVar("TSignedBeaconBlock", bound="BaseSignedBeaconBlock")


class BaseSignedBeaconBlock(HashableContainer):
    fields = [("message", BeaconBlock), ("signature", bytes96)]

    @classmethod
    def create(
        cls: Type[TSignedBeaconBlock],
        *,
        message: BaseBeaconBlock = default_beacon_block,
        signature: BLSSignature = EMPTY_SIGNATURE,
    ) -> TSignedBeaconBlock:
        return super().create(message=message, signature=signature)

    @property
    def parent_root(self) -> Root:
        return self.message.parent_root

    @property
    def is_genesis(self) -> bool:
        return self.message.is_genesis

    @property
    def slot(self) -> Slot:
        return self.message.slot

    @property
    def body(self) -> "BeaconBlockBody":
        return self.message.body

    def __str__(self) -> str:
        return (
            f"[hash_tree_root]={humanize_hash(self.message.hash_tree_root)},"
            f" slot={self.message.slot},"
            f" parent_root={humanize_hash(self.message.parent_root)},"
            f" state_root={humanize_hash(self.message.state_root)},"
            f" body=({self.message.body})"
        )

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}: {str(self)}>"


class SignedBeaconBlock(BaseSignedBeaconBlock):
    block_class = BeaconBlock

    @classmethod
    def from_root(cls, root: Root, chaindb: "BaseBeaconChainDB") -> "SignedBeaconBlock":
        block = cls.block_class.from_root(root, chaindb)
        return cls.create(message=block, signature=EMPTY_SIGNATURE)

    # TODO: add abstract class
    @classmethod
    def from_parent(
        cls: Type[TSignedBeaconBlock],
        parent_block: "BaseSignedBeaconBlock",
        block_params: FromBlockParams,
    ) -> TSignedBeaconBlock:

        block = cls.block_class.from_parent(parent_block.message, block_params)
        return cls.create(message=block, signature=EMPTY_SIGNATURE)

    @classmethod
    def from_header(
        cls: Type[TSignedBeaconBlock], header: SignedBeaconBlockHeader
    ) -> TSignedBeaconBlock:
        block = cls.block_class.from_header(header)
        return cls.create(message=block, signature=EMPTY_SIGNATURE)
