import ssz

from eth2.beacon.types.voluntary_exits import SignedVoluntaryExit, VoluntaryExit


def test_defaults(sample_voluntary_exit_params, sample_signature):
    exit = VoluntaryExit.create(**sample_voluntary_exit_params)
    signed_exit = SignedVoluntaryExit.create(message=exit, signature=sample_signature)

    assert exit.epoch == sample_voluntary_exit_params["epoch"]
    assert ssz.encode(exit)
    assert ssz.encode(signed_exit)
