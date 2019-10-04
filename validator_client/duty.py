from dataclasses import dataclass, field
from enum import Enum, auto, unique

from eth_typing import BLSPubkey
from eth_utils import humanize_hash

from eth2.beacon.signature_domain import SignatureDomain
from eth2.beacon.typing import CommitteeIndex, DomainType, Slot


@unique
class DutyType(Enum):
    Attestation = auto()
    BlockProposal = auto()


@dataclass(eq=True, frozen=True)
class Duty:
    """
    A ``Duty`` represents some work that needs to be performed on behalf of some
    validator, like signing an ``Attestation``.
    """

    validator_public_key: BLSPubkey
    slot: Slot


@dataclass(eq=True, frozen=True)
class AttestationDuty(Duty):
    duty_type: DutyType = field(init=False, default=DutyType.Attestation)
    domain_type: DomainType = field(
        init=False, default=SignatureDomain.DOMAIN_BEACON_ATTESTER
    )
    committee_index: CommitteeIndex

    def __repr__(self) -> str:
        return (
            f"AttestationDuty(validator_public_key={humanize_hash(self.validator_public_key)},"
            f" slot={self.slot}, committee_index={self.committee_index})"
        )


@dataclass(eq=True, frozen=True)
class BlockProposalDuty(Duty):
    duty_type: DutyType = field(init=False, default=DutyType.BlockProposal)
    domain_type: DomainType = field(
        init=False, default=SignatureDomain.DOMAIN_BEACON_PROPOSER
    )

    def __repr__(self) -> str:
        return (
            f"BlockProposalDuty(validator_public_key={humanize_hash(self.validator_public_key)},"
            f" slot={self.slot})"
        )
