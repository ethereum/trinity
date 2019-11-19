from enum import IntEnum, unique


@unique
class SignatureDomain(IntEnum):
    DOMAIN_BEACON_PROPOSER = 0
    DOMAIN_RANDAO = 1
    DOMAIN_ATTESTATION = 2
    DOMAIN_DEPOSIT = 3
    DOMAIN_VOLUNTARY_EXIT = 4
    DOMAIN_TRANSFER = 5
