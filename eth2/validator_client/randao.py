from eth_typing import BLSPubkey, BLSSignature
import ssz

from eth2._utils.bls import bls
from eth2.beacon.helpers import compute_domain
from eth2.beacon.signature_domain import SignatureDomain
from eth2.beacon.typing import Epoch
from eth2.validator_client.typing import PrivateKeyProvider, RandaoProvider


def mk_randao_provider(private_key_provider: PrivateKeyProvider) -> RandaoProvider:
    def _randao_provider_of_epoch_signature(
        public_key: BLSPubkey, epoch: Epoch
    ) -> BLSSignature:
        private_key = private_key_provider(public_key)
        # TODO: fix how we get the signing root
        message = ssz.get_hash_tree_root(epoch, sedes=ssz.sedes.uint64)
        domain = compute_domain(SignatureDomain.DOMAIN_RANDAO)
        sig = bls.sign(message, private_key, domain)
        return sig

    return _randao_provider_of_epoch_signature
