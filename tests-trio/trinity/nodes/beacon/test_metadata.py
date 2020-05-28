import ssz

from trinity.nodes.beacon.metadata import MetaData


def test_metadata_roundtrip():
    meta = MetaData.create(seq_number=2000, attnets=(True, False) * 32)
    meta_encoded = ssz.encode(meta)
    meta_recovered = ssz.decode(meta_encoded, MetaData)
    assert meta_recovered == meta
