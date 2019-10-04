from abc import abstractmethod
from typing import AsyncContextManager, Collection, Container, ContextManager

from eth_typing import BLSPubkey, BLSSignature

from eth2.beacon.types.attestations import Attestation
from eth2.beacon.types.blocks import BeaconBlock
from eth2.beacon.typing import CommitteeIndex, Epoch, Operation, Slot
from eth2.validator_client.duty import Duty
from eth2.validator_client.tick import Tick
from eth2.validator_client.typing import BLSPrivateKey


class BeaconNodeAPI(AsyncContextManager["BeaconNodeAPI"]):
    """
    ``BeaconNodeAPI`` represents a remote beacon node the validator client
    can query for information about the beacon state and supply
    signed messages to.
    """

    @abstractmethod
    async def fetch_duties(
        self,
        current_tick: Tick,
        public_keys: Collection[BLSPubkey],
        target_epoch: Epoch,
    ) -> Collection[Duty]:
        ...

    @abstractmethod
    async def fetch_attestation(
        self, public_key: BLSPubkey, slot: Slot, committee_index: CommitteeIndex
    ) -> Attestation:
        ...

    @abstractmethod
    async def fetch_block_proposal(
        self, public_key: BLSPubkey, slot: Slot
    ) -> BeaconBlock:
        ...

    @abstractmethod
    async def publish(self, duty: Duty, signature: BLSSignature) -> None:
        ...


class SignatoryDatabaseAPI(Container[bytes]):
    """
    Provides persistence for actions of the client to prevent
    the publishing of slashable signatures.
    """

    @abstractmethod
    async def record_signature_for(self, duty: Duty, operation: Operation) -> None:
        ...

    @abstractmethod
    async def is_slashable(self, duty: Duty, operation: Operation) -> bool:
        ...

    @abstractmethod
    def insert(self, key: bytes, value: bytes) -> None:
        ...


class KeyStoreAPI(ContextManager["KeyStoreAPI"]):
    @property
    @abstractmethod
    def public_keys(self) -> Collection[BLSPubkey]:
        ...

    @abstractmethod
    def import_private_key(self, encoded_private_key: str, password: bytes) -> None:
        ...

    @abstractmethod
    def private_key_for(self, public_key: BLSPubkey) -> BLSPrivateKey:
        ...
