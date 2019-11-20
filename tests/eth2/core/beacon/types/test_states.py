from eth2.beacon.types.states import BeaconState
import ssz


def test_defaults(sample_beacon_state_params):
    state = BeaconState.create(**sample_beacon_state_params)
    assert tuple(state.validators) == sample_beacon_state_params["validators"]
    assert ssz.encode(state)
