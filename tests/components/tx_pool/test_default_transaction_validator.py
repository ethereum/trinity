import pytest

from eth.chains.base import (
    MiningChain,
)
from eth.tools.builder.chain import api
from eth.vm.forks.frontier.transactions import (
    FrontierTransaction,
)
from eth.vm.forks.homestead.transactions import (
    HomesteadTransaction,
)
from eth.vm.forks.spurious_dragon.transactions import (
    SpuriousDragonTransaction,
)

from trinity._utils.transactions import (
    DefaultTransactionValidator,
)


@pytest.mark.parametrize(
    (
        'initial_block_number',
        'expected_initial_tx_builder',
        'expected_outdated_tx_builder',
        'expected_future_tx_builder',
    ),
    [
        (1, FrontierTransaction, None, SpuriousDragonTransaction,),
        (5, HomesteadTransaction, FrontierTransaction, SpuriousDragonTransaction,),
        (6, HomesteadTransaction, FrontierTransaction, SpuriousDragonTransaction,),
        # If no initial block number is specified, the latest VM is assumed to be active
        (None, SpuriousDragonTransaction, HomesteadTransaction, SpuriousDragonTransaction,),
    ],
)
def test_tx_builder_resolution(
        initial_block_number,
        expected_initial_tx_builder,
        expected_outdated_tx_builder,
        expected_future_tx_builder):

    chain = api.build(
        MiningChain,
        api.frontier_at(0),
        api.homestead_at(5),
        api.spurious_dragon_at(10),
        api.disable_pow_check,
        api.genesis
    )
    validator = DefaultTransactionValidator(chain, initial_block_number)
    assert validator.get_appropriate_tx_builder() == expected_initial_tx_builder

    if expected_outdated_tx_builder is not None:
        assert validator.is_outdated_tx_builder(expected_outdated_tx_builder)

    # The `get_appropriate_tx_builder` method has a cache decorator applied
    # To test it properly, we need to clear the cache
    validator.get_appropriate_tx_builder.cache_clear()

    # Check that the validator uses the correct tx class when we have reached the tip of the chain
    for _ in range(10):
        chain.mine_block()

    assert validator.get_appropriate_tx_builder() == expected_future_tx_builder
