from typing import Type, TypeVar

from eth.constants import ZERO_HASH32
from eth_typing import BLSSignature, Hash32
from eth_utils import humanize_hash
from ssz.hashable_container import HashableContainer
from ssz.sedes import bytes32, bytes96, uint64

from eth2.beacon.constants import EMPTY_SIGNATURE, ZERO_ROOT
from eth2.beacon.typing import Root, Slot

from .defaults import default_slot

TBeaconBlockHeader = TypeVar("TBeaconBlockHeader", bound="BeaconBlockHeader")


class BeaconBlockHeader(HashableContainer):

    fields = [
        ("slot", uint64),
        ("parent_root", bytes32),
        ("state_root", bytes32),
        ("body_root", bytes32),
    ]

    @classmethod
    def create(
        cls: Type[TBeaconBlockHeader],
        *,
        slot: Slot = default_slot,
        parent_root: Root = ZERO_ROOT,
        state_root: Hash32 = ZERO_HASH32,
        body_root: Hash32 = ZERO_HASH32,
    ) -> TBeaconBlockHeader:
        return super().create(
            slot=slot,
            parent_root=parent_root,
            state_root=state_root,
            body_root=body_root,
        )

    def __str__(self) -> str:
        return (
            f"[hash_tree_root]={humanize_hash(self.hash_tree_root)},"
            f" slot={self.slot},"
            f" parent_root={humanize_hash(self.parent_root)},"
            f" state_root={humanize_hash(self.state_root)},"
            f" body_root={humanize_hash(self.body_root)},"
        )

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}: {str(self)}>"


default_beacon_block_header = BeaconBlockHeader.create()

TSignedBeaconBlockHeader = TypeVar(
    "TSignedBeaconBlockHeader", bound="SignedBeaconBlockHeader"
)


class SignedBeaconBlockHeader(HashableContainer):
    fields = [("message", BeaconBlockHeader), ("signature", bytes96)]

    @classmethod
    def create(
        cls: Type[TSignedBeaconBlockHeader],
        *,
        message: BeaconBlockHeader = default_beacon_block_header,
        signature: BLSSignature = EMPTY_SIGNATURE,
    ) -> TSignedBeaconBlockHeader:
        return super().create(message=message, signature=signature)


default_signed_beacon_block_header = SignedBeaconBlockHeader.create()
