from abc import (
    ABC,
    abstractmethod,
)

from typing import (
    Sequence,
    TYPE_CHECKING,
)

from eth.constants import (
    ZERO_HASH32,
)

from eth_typing import (
    BLSSignature,
    Hash32,
)
from eth_utils import (
    encode_hex,
)

import ssz
from ssz.sedes import (
    List,
    bytes32,
    bytes96,
    uint64,
)


from eth._utils.datatypes import (
    Configurable,
)

from eth2.beacon.constants import EMPTY_SIGNATURE
from eth2.beacon.typing import (
    Slot,
    FromBlockParams,
)


from .attestations import Attestation
from .attester_slashings import AttesterSlashing
from .deposits import Deposit
from .eth1_data import Eth1Data
from .transfers import Transfer
from .voluntary_exits import VoluntaryExit

if TYPE_CHECKING:
    from .proposer_slashings import ProposerSlashing  # noqa: F401
    from eth2.beacon.db.chain import BaseBeaconChainDB  # noqa: F401


class BeaconBlockHeader(ssz.Serializable):

    fields = [
        ('slot', uint64),
        ('previous_block_root', bytes32),
        ('state_root', bytes32),
        ('block_body_root', bytes32),
        ('signature', bytes96),
    ]

    def __init__(self,
                 *,
                 slot: uint64,
                 previous_block_root: bytes32,
                 state_root: bytes32,
                 block_body_root: bytes32,
                 signature: BLSSignature=EMPTY_SIGNATURE):
        super().__init__(
            slot=slot,
            previous_block_root=previous_block_root,
            state_root=state_root,
            block_body_root=block_body_root,
            signature=signature,
        )

    def __repr__(self) -> str:
        return '<BlockHeader #{0} {1}>'.format(
            self.slot,
            encode_hex(self.signed_root)[2:10],
        )

    _root = None

    @property
    def root(self) -> Hash32:
        return super().root

    _signed_root = None

    @property
    def signed_root(self) -> Hash32:
        # TODO Use SSZ built-in function
        if self._signed_root is None:
            self._signed_root = ssz.hash_tree_root(self.copy(signature=EMPTY_SIGNATURE))
        return Hash32(self._signed_root)


class BeaconBlockBody(ssz.Serializable):

    fields = [
        ('randao_reveal', bytes96),
        ('eth1_data', Eth1Data),
        ('proposer_slashings', List('ProposerSlashing')),
        ('attester_slashings', List(AttesterSlashing)),
        ('attestations', List(Attestation)),
        ('deposits', List(Deposit)),
        ('voluntary_exits', List(VoluntaryExit)),
        ('transfers', List(Transfer)),
    ]

    def __init__(self,
                 *,
                 randao_reveal: bytes96,
                 eth1_data: Eth1Data,
                 proposer_slashings: Sequence['ProposerSlashing'],
                 attester_slashings: Sequence[AttesterSlashing],
                 attestations: Sequence[Attestation],
                 deposits: Sequence[Deposit],
                 voluntary_exits: Sequence[VoluntaryExit],
                 transfers: Sequence[Transfer])-> None:
        super().__init__(
            randao_reveal=randao_reveal,
            eth1_data=eth1_data,
            proposer_slashings=proposer_slashings,
            attester_slashings=attester_slashings,
            attestations=attestations,
            deposits=deposits,
            voluntary_exits=voluntary_exits,
            transfers=transfers,
        )

    @classmethod
    def create_empty_body(cls) -> 'BeaconBlockBody':
        return cls(
            randao_reveal=EMPTY_SIGNATURE,
            eth1_data=Eth1Data.create_empty_data(),
            proposer_slashings=(),
            attester_slashings=(),
            attestations=(),
            deposits=(),
            voluntary_exits=(),
            transfers=(),
        )

    @property
    def is_empty(self) -> bool:
        return (
            self.randao_reveal == EMPTY_SIGNATURE and
            self.eth1_data == Eth1Data.create_empty_data() and
            self.proposer_slashings == () and
            self.attester_slashings == () and
            self.attestations == () and
            self.deposits == () and
            self.voluntary_exits == () and
            self.transfers == ()
        )

    @classmethod
    def cast_block_body(cls,
                        body: 'BeaconBlockBody') -> 'BeaconBlockBody':
        return cls(
            randao_reveal=body.randao_reveal,
            eth1_data=body.eth1_data,
            proposer_slashings=body.proposer_slashings,
            attester_slashings=body.attester_slashings,
            attestations=body.attestations,
            deposits=body.deposits,
            voluntary_exits=body.voluntary_exits,
            transfers=body.transfers,
        )

    _root = None

    @property
    def root(self) -> Hash32:
        return super().root


class BaseBeaconBlock(ssz.Serializable, Configurable, ABC):
    fields = [
        #
        # Header
        #
        ('slot', uint64),
        ('previous_block_root', bytes32),
        ('state_root', bytes32),

        #
        # Body
        #
        ('body', BeaconBlockBody),

        ('signature', bytes96),
    ]

    def __init__(self,
                 *,
                 slot: Slot,
                 previous_block_root: Hash32,
                 state_root: Hash32,
                 body: BeaconBlockBody,
                 signature: BLSSignature=EMPTY_SIGNATURE) -> None:
        super().__init__(
            slot=slot,
            previous_block_root=previous_block_root,
            state_root=state_root,
            body=body,
            signature=signature,
        )

    def __repr__(self) -> str:
        return '<Block #{0} {1}>'.format(
            self.slot,
            encode_hex(self.signed_root)[2:10],
        )

    _root = None

    @property
    def root(self) -> Hash32:
        return super().root

    _signed_root = None

    @property
    def signed_root(self) -> Hash32:
        # TODO Use SSZ built-in function
        if self._signed_root is None:
            self._signed_root = ssz.hash_tree_root(self.copy(signature=EMPTY_SIGNATURE))
        return Hash32(self._signed_root)

    @property
    def num_attestations(self) -> int:
        return len(self.body.attestations)

    @property
    def header(self) -> BeaconBlockHeader:
        return BeaconBlockHeader(
            slot=self.slot,
            previous_block_root=self.previous_block_root,
            state_root=self.state_root,
            block_body_root=self.body.root,
            signature=self.signature,
        )

    @classmethod
    @abstractmethod
    def from_root(cls, root: Hash32, chaindb: 'BaseBeaconChainDB') -> 'BaseBeaconBlock':
        """
        Return the block denoted by the given block root.
        """
        raise NotImplementedError("Must be implemented by subclasses")

    @classmethod
    def create_empty_block(cls, genesis_slot: Slot) -> 'BaseBeaconBlock':
        return cls(
            slot=genesis_slot,
            previous_block_root=ZERO_HASH32,
            state_root=ZERO_HASH32,
            body=BeaconBlockBody.create_empty_body(),
            signature=EMPTY_SIGNATURE,
        )


class BeaconBlock(BaseBeaconBlock):
    block_body_class = BeaconBlockBody

    @classmethod
    def from_root(cls, root: Hash32, chaindb: 'BaseBeaconChainDB') -> 'BeaconBlock':
        """
        Return the block denoted by the given block ``root``.
        """
        block = chaindb.get_block_by_root(root, cls)
        body = cls.block_body_class(
            randao_reveal=block.body.randao_reveal,
            eth1_data=block.body.eth1_data,
            proposer_slashings=block.body.proposer_slashings,
            attester_slashings=block.body.attester_slashings,
            attestations=block.body.attestations,
            deposits=block.body.deposits,
            voluntary_exits=block.body.voluntary_exits,
            transfers=block.body.transfer,
        )

        return cls(
            slot=block.slot,
            previous_block_root=block.previous_block_root,
            state_root=block.state_root,
            body=body,
            signature=block.signature,
        )

    @classmethod
    def from_parent(cls,
                    parent_block: 'BaseBeaconBlock',
                    block_params: FromBlockParams) -> 'BaseBeaconBlock':
        """
        Initialize a new block with the ``parent_block`` as the block's
        previous block root.
        """
        if block_params.slot is None:
            slot = parent_block.slot + 1
        else:
            slot = block_params.slot

        return cls(
            slot=slot,
            previous_block_root=parent_block.signed_root,
            state_root=parent_block.state_root,
            body=cls.block_body_class.create_empty_body(),
            signature=EMPTY_SIGNATURE,
        )

    @classmethod
    def convert_block(cls,
                      block: 'BaseBeaconBlock') -> 'BeaconBlock':
        return cls(
            slot=block.slot,
            previous_block_root=block.previous_block_root,
            state_root=block.state_root,
            body=block.body,
            signature=block.signature,
        )
