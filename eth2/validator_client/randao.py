from eth_typing import BLSPubkey, BLSSignature, Hash32

from eth2._utils.bls import bls
from eth2.beacon.helpers import compute_domain, compute_signing_root
from eth2.beacon.signature_domain import SignatureDomain
from eth2.beacon.typing import Epoch, Root, SerializableUint64
from eth2.validator_client.typing import PrivateKeyProvider, RandaoProvider


def mk_randao_provider(private_key_provider: PrivateKeyProvider) -> RandaoProvider:
    def _randao_provider_of_epoch_signature(
        public_key: BLSPubkey, epoch: Epoch
    ) -> BLSSignature:
        privkey = private_key_provider(public_key)
        # NOTE: hardcoded for testing, based on generating the minimal set of validators
        genesis_validators_root = Root(
            Hash32(
                bytes.fromhex(
                    "83431ec7fcf92cfc44947fc0418e831c25e1d0806590231c439830db7ad54fda"
                )
            )
        )
        domain = compute_domain(
            SignatureDomain.DOMAIN_RANDAO,
            genesis_validators_root=genesis_validators_root,
        )
        signing_root = compute_signing_root(SerializableUint64(epoch), domain)
        return bls.sign(privkey, signing_root)

    return _randao_provider_of_epoch_signature
