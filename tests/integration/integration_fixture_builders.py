from eth.tools.builder.chain import byzantium_at
from eth_keys import keys
from eth_utils import decode_hex

from tests.core.integration_test_helpers import FUNDED_ACCT, load_mining_chain

from .churn_state.builder import (
    add_nymph_contract,
    churn_storage_once,
    delete_churn,
    deploy_storage_churn_contract,
    update_churn,
)

RECEIVER = keys.PrivateKey(
    decode_hex("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291"))


def build_uncle_chain_to_existing_db(write_db, existing_db, fork_at, num_blocks=20):
    # 1000pow_headers fixture uses byzantium and we don't want to upgrade the fixture to not bloat
    # the git repository too much (see discussion: https://github.com/ethereum/trinity/pull/1777)
    base_chain = load_mining_chain(existing_db, byzantium_at(0))
    uncle_chain = load_mining_chain(write_db, byzantium_at(0))

    assert uncle_chain.get_canonical_head() == base_chain.get_canonical_block_header_by_number(0)
    for block_number in range(1, fork_at):
        uncle_chain.import_block(base_chain.get_canonical_block_by_number(block_number))

    assert uncle_chain.get_canonical_head().block_number == fork_at - 1

    for _ in range(num_blocks):
        uncle_chain.mine_block()

    # If `fork_at` is 500 it means that up to block 499 is shared history and 500 is the first uncle
    tip_number = fork_at - 1 + num_blocks

    assert uncle_chain.get_canonical_head().block_number == tip_number
    assert uncle_chain.get_canonical_head() != base_chain.get_canonical_block_by_number(tip_number)


def build_pow_fixture(write_db, num_blocks=20):
    chain = load_mining_chain(write_db)
    recipient_address = RECEIVER.public_key.to_canonical_address()
    for i in range(num_blocks):
        tx = chain.create_unsigned_transaction(
            nonce=i,
            gas_price=1234,
            gas=123400,
            to=recipient_address,
            value=i,
            data=b'',
        )
        chain.apply_transaction(tx.as_signed_transaction(FUNDED_ACCT))
        chain.mine_block()
    return chain.chaindb


def build_pow_churning_fixture(write_db, num_blocks=40):
    chain = load_mining_chain(write_db)
    contract_addr = deploy_storage_churn_contract(chain)
    half_blocks = num_blocks // 2
    nymph_contracts, nonce = update_churn(chain, contract_addr, num_blocks=half_blocks)
    delete_churn(chain, nymph_contracts, contract_addr, start_nonce=nonce, num_blocks=half_blocks)
    return chain, contract_addr


def build_pow_cold_state_fixture(write_db, num_blocks=10):
    chain = load_mining_chain(write_db)

    # Reuse the churning methods for convenience, rather than because they're
    #   specially suited to the task

    # Deploy three contracts to fill with data, then never touch the first two
    cold1, cold2, active = [deploy_storage_churn_contract(chain, nonce) for nonce in range(3)]

    chain.mine_block()

    # Fill in some cold data
    churn_storage_once(chain, cold1, nonce=3)
    churn_storage_once(chain, cold2, nonce=4)
    churn_storage_once(chain, cold2, nonce=5)

    chain.mine_block()

    # Add some unique bytecodes that are not called later in the chain
    # This is to test if bytecodes get backfilled
    add_nymph_contract(chain, nonce=6, variant=1)
    add_nymph_contract(chain, nonce=7, variant=2)

    chain.mine_block()

    # Create a bunch of accounts, just to branch out the address space a bit
    for nonce in range(8, 128):
        add_nymph_contract(chain, nonce)
        if nonce % 35 == 5:
            chain.mine_block()

    chain.mine_block()

    update_churn(chain, active, start_nonce=128, num_blocks=num_blocks)
    return chain, (cold1, cold2, active)
