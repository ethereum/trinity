from typing import Callable, Tuple

from eth_typing import BLSPubkey, BLSSignature

from eth2.beacon.typing import Epoch, Operation
from eth2.validator_client.duty import Duty

BLSPrivateKey = int

KeyPair = Tuple[BLSPubkey, BLSPrivateKey]

PrivateKeyProvider = Callable[[BLSPubkey], BLSPrivateKey]

RandaoProvider = Callable[[BLSPubkey, Epoch], BLSSignature]

ResolvedDuty = Tuple[Duty, Operation]
