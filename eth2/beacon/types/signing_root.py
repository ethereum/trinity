from typing import Type, TypeVar

from eth.constants import ZERO_HASH32
from eth_utils import humanize_hash
from ssz.hashable_container import HashableContainer
from ssz.sedes import bytes32

from eth2.beacon.typing import Root, Domain

TSigningRoot = TypeVar("TSigningRoot", bound="SigningRoot")


class SigningRoot(HashableContainer):

    fields = [
        ("object_root", bytes32),
        ("domain", bytes32),
    ]

    @classmethod
    def create(
            cls: Type[TSigningRoot],
            object_root: Root = ZERO_HASH32,
            domain: Domain = ZERO_HASH32,
    ) -> TSigningRoot:
        return super().create(
            object_root=object_root,
            domain=domain,
        )

    def __str__(self) -> str:
        return (
            f"object_root={humanize_hash(self.object_root)},"
            f" domain={humanize_hash(self.domain)},"
        )


default_signing_root = SigningRoot.create()
