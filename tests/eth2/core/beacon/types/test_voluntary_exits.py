import ssz

from eth2.beacon.types.voluntary_exits import VoluntaryExit, SignedVoluntaryExit


def test_defaults(sample_voluntary_exit_params, sample_signature):
    exit = VoluntaryExit.create(**sample_voluntary_exit_params)

    assert exit.epoch == sample_voluntary_exit_params["epoch"]
    assert ssz.encode(exit)
