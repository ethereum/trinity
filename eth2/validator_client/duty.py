from dataclasses import dataclass, field
from enum import Enum, auto, unique

from eth_typing import BLSPubkey

from eth2._utils.humanize import humanize_bytes
from eth2.beacon.signature_domain import SignatureDomain
from eth2.beacon.typing import CommitteeIndex
from eth2.validator_client.tick import Tick


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
    # ``Tick`` when this ``Duty`` should be executed, resulting in a
    # (slashable) signature to publish
    tick_for_execution: Tick
    # ``Tick`` when this ``Duty`` was discovered via a ``BeaconNode``
    discovered_at_tick: Tick
    duty_type: DutyType
    signature_domain: SignatureDomain


@dataclass(eq=True, frozen=True)
class AttestationDuty(Duty):
    duty_type: DutyType = field(init=False, default=DutyType.Attestation)
    signature_domain: SignatureDomain = field(
        init=False, default=SignatureDomain.DOMAIN_BEACON_ATTESTER
    )
    committee_index: CommitteeIndex

    def __repr__(self) -> str:
        return (
            f"AttestationDuty(validator_public_key={humanize_bytes(self.validator_public_key)},"
            f" tick_for_execution={self.tick_for_execution},"
            f" discovered_at_tick={self.discovered_at_tick},"
            f" committee_index={self.committee_index})"
        )


@dataclass(eq=True, frozen=True)
class BlockProposalDuty(Duty):
    duty_type: DutyType = field(init=False, default=DutyType.BlockProposal)
    signature_domain: SignatureDomain = field(
        init=False, default=SignatureDomain.DOMAIN_BEACON_PROPOSER
    )

    def __repr__(self) -> str:
        return (
            f"BlockProposalDuty(validator_public_key={humanize_bytes(self.validator_public_key)},"
            f" tick_for_execution={self.tick_for_execution},"
            f" discovered_at_tick={self.discovered_at_tick}"
        )
