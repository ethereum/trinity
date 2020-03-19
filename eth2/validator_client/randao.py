from eth_typing import BLSPubkey, BLSSignature, Hash32
from py_ecc.bls.typing import Domain

from eth2._utils.bls import bls
from eth2.beacon.helpers import signature_domain_to_domain_type
from eth2.beacon.signature_domain import SignatureDomain
from eth2.beacon.typing import Epoch
from eth2.validator_client.typing import PrivateKeyProvider, RandaoProvider


def mk_randao_provider(private_key_provider: PrivateKeyProvider) -> RandaoProvider:
    def _randao_provider_of_epoch_signature(
        public_key: BLSPubkey, epoch: Epoch
    ) -> BLSSignature:
        private_key = private_key_provider(public_key)
        # TODO: fix how we get the signing root
        message = Hash32(epoch.to_bytes(32, byteorder="big"))
        domain = Domain(
            b"\x00" * 4 + signature_domain_to_domain_type(SignatureDomain.DOMAIN_RANDAO)
        )
        sig = bls.sign(message, private_key, domain)
        return sig

    return _randao_provider_of_epoch_signature
