from typing import Any, Callable, Dict, Iterable, Sequence, Tuple, Type

from eth.constants import ZERO_HASH32
from eth_typing import BLSPubkey, Hash32
from eth_utils import decode_hex, encode_hex, to_dict, to_tuple
from eth_utils.toolz import curry
from py_ecc.optimized_bls12_381.optimized_curve import (
    curve_order as BLS12_381_CURVE_ORDER,
)

from eth2._utils.bls import bls
from eth2._utils.hash import hash_eth2
from eth2._utils.merkle.common import MerkleTree, get_merkle_proof
from eth2.beacon.constants import ZERO_TIMESTAMP
from eth2.beacon.genesis import get_genesis_block, initialize_beacon_state_from_eth1
from eth2.beacon.tools.builder.validator import (
    create_deposit_data,
    create_mock_deposit_data,
)
from eth2.beacon.types.blocks import BaseBeaconBlock
from eth2.beacon.types.deposit_data import DepositData  # noqa: F401
from eth2.beacon.types.deposits import Deposit
from eth2.beacon.types.eth1_data import Eth1Data
from eth2.beacon.types.states import BeaconState
from eth2.beacon.types.validators import Validator
from eth2.beacon.typing import Gwei, Timestamp
from eth2.beacon.validator_status_helpers import activate_validator
from eth2.configs import Eth2Config

from .validator import make_deposit_proof, make_deposit_tree_and_root


def _encode_private_key_as_hex(private_key: int) -> str:
    return encode_hex(private_key.to_bytes(32, byteorder="little"))


def _decode_private_key_from_hex(encoded_private_key: str) -> int:
    return int.from_bytes(decode_hex(encoded_private_key), byteorder="little")


def mk_genesis_key_map(
    key_pairs: Dict[BLSPubkey, int],
    genesis_state: BeaconState,
    public_key_codec: Callable[[BLSPubkey], str] = encode_hex,
    private_key_codec: Callable[[int], str] = _encode_private_key_as_hex,
) -> Tuple[Dict[str, str], ...]:
    key_map: Tuple[Dict[str, str], ...] = ()
    for _, validator in enumerate(genesis_state.validators):
        public_key = validator.pubkey
        private_key = key_pairs[public_key]
        key_map += (
            {
                "public_key": public_key_codec(public_key),
                "private_key": private_key_codec(private_key),
            },
        )
    return key_map


def load_genesis_key_map(
    encoded_key_map: Tuple[Dict[str, Any]],
    public_key_codec: Callable[[str], BLSPubkey] = lambda key: BLSPubkey(
        decode_hex(key)
    ),
    private_key_codec: Callable[[str], int] = _decode_private_key_from_hex,
) -> Dict[BLSPubkey, int]:
    key_map = {}
    for key_pair in encoded_key_map:
        public_key = public_key_codec(key_pair["public_key"])
        private_key = private_key_codec(key_pair["private_key"])
        key_map[public_key] = private_key
    return key_map


def generate_privkey_from_index(index: int) -> int:
    return (
        int.from_bytes(
            hash_eth2(index.to_bytes(length=32, byteorder="little")), byteorder="little"
        )
        % BLS12_381_CURVE_ORDER
    )


def create_keypair_and_mock_withdraw_credentials(
    config: Eth2Config, key_set: Sequence[Dict[str, Any]]
) -> Tuple[Tuple[BLSPubkey, ...], Tuple[int, ...], Tuple[Hash32, ...]]:
    """
    NOTE: this function mixes the parsing of keying material with the generation of derived values.
    Prefer other functions in this module that do the derivation directly.
    """
    pubkeys: Tuple[BLSPubkey, ...] = ()
    privkeys: Tuple[int, ...] = ()
    withdrawal_credentials: Tuple[Hash32, ...] = ()
    for key_pair in key_set:
        pubkey = BLSPubkey(decode_hex(key_pair["pubkey"]))
        privkey = int.from_bytes(decode_hex(key_pair["privkey"]), "big")
        withdrawal_credential = Hash32(
            config.BLS_WITHDRAWAL_PREFIX.to_bytes(1, byteorder="big")
            + hash_eth2(pubkey)[1:]
        )

        pubkeys += (pubkey,)
        privkeys += (privkey,)
        withdrawal_credentials += (withdrawal_credential,)

    return (pubkeys, privkeys, withdrawal_credentials)


@to_dict
def create_key_pairs_for(validator_count: int) -> Iterable[Tuple[BLSPubkey, int]]:
    """
    Generates ``validator_count`` key pairs derived in a deterministic manner based
    on the validator index in the ``range(validator_count)``.

    Returns a second map associating a public key with the validator's index in the set.
    """
    for i in range(validator_count):
        private_key = generate_privkey_from_index(i)
        public_key = bls.privtopub(private_key)
        yield public_key, private_key


@curry
def mk_withdrawal_credentials_from(prefix: bytes, public_key: BLSPubkey) -> Hash32:
    return Hash32(prefix + hash_eth2(public_key)[1:])


def create_deposit_proof(
    tree: MerkleTree, index: int, leaf_count: int
) -> Tuple[Hash32, ...]:
    length_mix_in = Hash32(leaf_count.to_bytes(32, byteorder="little"))
    proof = get_merkle_proof(tree, index)
    return proof + (length_mix_in,)


def create_deposit(
    deposit_data: DepositData, tree: MerkleTree, index: int, leaf_count: int
) -> Deposit:
    proof = create_deposit_proof(tree, index, leaf_count)
    return Deposit.create(proof=proof, data=deposit_data)


@to_tuple
def create_genesis_deposits_from(
    key_pairs: Dict[BLSPubkey, int],
    withdrawal_credentials_provider: Callable[[BLSPubkey], Hash32],
    amount_provider: Callable[[BLSPubkey], Gwei],
) -> Iterable[Deposit]:
    deposit_data = tuple(
        create_deposit_data(
            public_key,
            private_key,
            withdrawal_credentials_provider(public_key),
            amount_provider(public_key),
        )
        for public_key, private_key in key_pairs.items()
    )

    for index, data in enumerate(deposit_data):
        data_to_index = deposit_data[: index + 1]
        tree, _ = make_deposit_tree_and_root(data_to_index)
        yield create_deposit(data, tree, index, len(data_to_index))


def create_mock_deposits_and_root(
    pubkeys: Sequence[BLSPubkey],
    keymap: Dict[BLSPubkey, int],
    config: Eth2Config,
    withdrawal_credentials: Sequence[Hash32] = None,
    leaves: Sequence[Hash32] = None,
) -> Tuple[Tuple[Deposit, ...], Hash32]:
    """
    Creates as many new deposits as there are keys in ``pubkeys``.

    Optionally provide corresponding ``withdrawal_credentials`` to include those.

    Optionally provide the prefix in the sequence of leaves leading up to the
    new deposits made by this function to get the correct updated root. If ``leaves`` is
    empty, this function simulates the genesis deposit tree calculation.
    """
    if not withdrawal_credentials:
        withdrawal_credentials = tuple(
            Hash32(b"\x22" * 32) for _ in range(len(pubkeys))
        )
    else:
        assert len(withdrawal_credentials) == len(pubkeys)
    if not leaves:
        leaves = tuple()

    deposit_datas = tuple()  # type: Tuple[DepositData, ...]
    for key, credentials in zip(pubkeys, withdrawal_credentials):
        privkey = keymap[key]
        deposit_data = create_mock_deposit_data(
            config=config,
            pubkey=key,
            privkey=privkey,
            withdrawal_credentials=credentials,
        )
        deposit_datas += (deposit_data,)

    deposits: Tuple[Deposit, ...] = tuple()
    for index, data in enumerate(deposit_datas):
        deposit_datas_at_count = deposit_datas[: index + 1]
        tree, root = make_deposit_tree_and_root(deposit_datas_at_count)
        proof = make_deposit_proof(deposit_datas_at_count, tree, root, index)

        deposit = Deposit.create(proof=proof, data=data)
        deposits += (deposit,)

    if len(deposit_datas) > 0:
        return deposits, root
    else:
        return tuple(), ZERO_HASH32


def create_mock_deposit(
    state: BeaconState,
    pubkey: BLSPubkey,
    keymap: Dict[BLSPubkey, int],
    withdrawal_credentials: Hash32,
    config: Eth2Config,
    leaves: Sequence[Hash32] = None,
) -> Tuple[BeaconState, Deposit]:
    deposits, root = create_mock_deposits_and_root(
        (pubkey,),
        keymap,
        config,
        withdrawal_credentials=(withdrawal_credentials,),
        leaves=leaves,
    )
    # sanity check
    assert len(deposits) == 1
    deposit = deposits[0]

    eth1_data = state.eth1_data.mset(
        "deposit_root",
        root,
        "deposit_count",
        state.eth1_data.deposit_count + len(deposits),
    )
    state = state.mset(
        "eth1_data", eth1_data, "eth1_deposit_index", 0 if not leaves else len(leaves)
    )

    return state, deposit


def create_mock_genesis(
    pubkeys: Sequence[BLSPubkey],
    config: Eth2Config,
    keymap: Dict[BLSPubkey, int],
    genesis_block_class: Type[BaseBeaconBlock],
    genesis_time: Timestamp = ZERO_TIMESTAMP,
) -> Tuple[BeaconState, BaseBeaconBlock]:
    genesis_deposits, deposit_root = create_mock_deposits_and_root(
        pubkeys=pubkeys, keymap=keymap, config=config
    )

    genesis_eth1_data = Eth1Data.create(
        deposit_root=deposit_root,
        deposit_count=len(genesis_deposits),
        block_hash=ZERO_HASH32,
    )

    state = initialize_beacon_state_from_eth1(
        eth1_block_hash=genesis_eth1_data.block_hash,
        eth1_timestamp=genesis_time,
        deposits=genesis_deposits,
        config=config,
    )

    block = get_genesis_block(
        genesis_state_root=state.hash_tree_root, block_class=genesis_block_class
    )
    assert len(state.validators) == len(pubkeys)

    return state, block


def create_mock_validator(
    pubkey: BLSPubkey,
    config: Eth2Config,
    withdrawal_credentials: Hash32 = ZERO_HASH32,
    is_active: bool = True,
) -> Validator:
    validator = Validator.create_pending_validator(
        pubkey, withdrawal_credentials, config.MAX_EFFECTIVE_BALANCE, config
    )
    if is_active:
        return activate_validator(validator, config.GENESIS_EPOCH)
    else:
        return validator
