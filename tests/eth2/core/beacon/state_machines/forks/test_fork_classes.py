import pytest

from eth2.beacon.fork_choice import higher_slot_scoring
from eth2.beacon.state_machines.forks.serenity import (
    SerenityStateMachine,
)
from eth2.beacon.state_machines.forks.xiao_long_bao import (
    XiaoLongBaoStateMachine,
)


@pytest.mark.parametrize(
    "sm_klass",
    (
        SerenityStateMachine,
        XiaoLongBaoStateMachine,
    )
)
def test_sm_class_well_defined(sm_klass):
    state_machine = sm_klass(chaindb=None, block=None, fork_choice_rule=higher_slot_scoring)
    assert state_machine.get_block_class()
