from typing import Callable, Tuple

from eth_typing import BLSPubkey

from eth2.beacon.typing import Operation
from eth2.validator_client.duty import Duty

BLSPrivateKey = int

KeyPair = Tuple[BLSPubkey, BLSPrivateKey]

PrivateKeyProvider = Callable[[BLSPubkey], BLSPrivateKey]

ResolvedDuty = Tuple[Duty, Operation]
