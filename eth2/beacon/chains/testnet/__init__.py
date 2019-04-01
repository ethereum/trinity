from typing import Tuple, Type   # noqa: F401
from .constants import (
    GENESIS_SLOT,
    TESTNET_CHAIN_ID,
)
from eth2.beacon.state_machines.forks.xiao_long_bao import (
    XiaoLongBaoStateMachine,
)
from eth2.beacon.chains.base import BeaconChain
from eth2.beacon.state_machines.base import BaseBeaconStateMachine  # noqa: F401
from eth2.beacon.typing import (  # noqa: F401
    Slot,
)


TESTNET_SM_CONFIGURATION = (
    (GENESIS_SLOT, XiaoLongBaoStateMachine),
)


class BaseTestnetChain:
    sm_configuration = TESTNET_SM_CONFIGURATION  # type: Tuple[Tuple[Slot, Type[BaseBeaconStateMachine]], ...] # noqa: E501
    chain_id = TESTNET_CHAIN_ID  # type: int


class TestnetChain(BaseTestnetChain, BeaconChain):
    pass
