from typing import TYPE_CHECKING
from eth2.beacon.chains.base import BeaconChain
from eth2.beacon.state_machines.forks.xiao_long_bao import XiaoLongBaoStateMachine
from eth2.beacon.state_machines.forks.serenity.configs import SERENITY_CONFIG
from .constants import TESTNET_CHAIN_ID

if TYPE_CHECKING:
    from eth2.beacon.typing import Slot  # noqa: F401
    from eth2.beacon.state_machines.base import BaseBeaconStateMachine  # noqa: F401
    from typing import Tuple, Type  # noqa: F401


TESTNET_SM_CONFIGURATION = (
    # FIXME: Shouldn't access GENESIS_SLOT from a particular state machine configs.
    (SERENITY_CONFIG.GENESIS_SLOT, XiaoLongBaoStateMachine),
)  # type: Tuple[Tuple[Slot, Type[BaseBeaconStateMachine]], ...]


class BaseTestnetChain:
    sm_configuration = TESTNET_SM_CONFIGURATION
    chain_id = TESTNET_CHAIN_ID


class TestnetChain(BaseTestnetChain, BeaconChain):
    pass
