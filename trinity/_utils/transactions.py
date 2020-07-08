from eth.abc import ChainAPI

from eth.chains.mainnet import MUIR_GLACIER_MAINNET_BLOCK
from eth.chains.ropsten import MUIR_GLACIER_ROPSTEN_BLOCK
from eth.chains.goerli import ISTANBUL_GOERLI_BLOCK

from trinity.components.builtin.tx_pool.validators import DefaultTransactionValidator

from trinity.constants import (
    GOERLI_NETWORK_ID,
    MAINNET_NETWORK_ID,
    ROPSTEN_NETWORK_ID
)


def get_transaction_validator_for_network_id(chain: ChainAPI,
                                             network_id: int) -> DefaultTransactionValidator:

    if network_id == MAINNET_NETWORK_ID:
        return DefaultTransactionValidator(chain, MUIR_GLACIER_MAINNET_BLOCK)
    elif network_id == ROPSTEN_NETWORK_ID:
        return DefaultTransactionValidator(chain, MUIR_GLACIER_ROPSTEN_BLOCK)
    elif network_id == GOERLI_NETWORK_ID:
        return DefaultTransactionValidator(chain, ISTANBUL_GOERLI_BLOCK)
    else:
        raise Exception("This code path should not be reachable")
