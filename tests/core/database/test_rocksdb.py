import rocksdb


def test_rocksdb_works(tmpdir):
    db = rocksdb.DB(str(tmpdir), rocksdb.Options(create_if_missing=True))
    db.put(b"a", b"b")
    assert db.get(b"a") == b"b"
