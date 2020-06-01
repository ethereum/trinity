from eth_typing import BLSPubkey, BLSSignature

from eth2._utils.bls import bls
from eth2.beacon.helpers import compute_domain, compute_signing_root
from eth2.beacon.signature_domain import SignatureDomain
from eth2.beacon.typing import Epoch, EpochOperation
from eth2.validator_client.typing import PrivateKeyProvider, RandaoProvider


def mk_randao_provider(private_key_provider: PrivateKeyProvider) -> RandaoProvider:
    def _randao_provider_of_epoch_signature(
        public_key: BLSPubkey, epoch: Epoch
    ) -> BLSSignature:
        privkey = private_key_provider(public_key)
        domain = compute_domain(SignatureDomain.DOMAIN_RANDAO)
        signing_root = compute_signing_root(EpochOperation(epoch), domain)
        return bls.Sign(privkey, signing_root)

    return _randao_provider_of_epoch_signature
