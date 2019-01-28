from eth2.beacon.typing import (
    BLSSignature,
    SlotNumber,
    EpochNumber,
)


#
# shuffle function
#

# The size of 3 bytes in integer
# sample_range = 2 ** (3 * 8) = 2 ** 24 = 16777216
# sample_range = 16777216

# Entropy is consumed from the seed in 3-byte (24 bit) chunks.
RAND_BYTES = 3
# The highest possible result of the RNG.
RAND_MAX = 2 ** (RAND_BYTES * 8) - 1

EMPTY_SIGNATURE = BLSSignature(b'\x00' * 96)

GWEI_PER_ETH = 10**9

TWO_POWER_64 = 2**64

# TODO: remove FAR_FUTURE_SLOT in other PR.
FAR_FUTURE_SLOT = SlotNumber(TWO_POWER_64 - 1)
FAR_FUTURE_EPOCH = EpochNumber(TWO_POWER_64 - 1)


GENESIS_SLOT = SlotNumber(2**19)
