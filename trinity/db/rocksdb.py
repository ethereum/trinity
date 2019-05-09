from contextlib import contextmanager
import logging
from pathlib import Path
from typing import (
    Iterator,
)

import cachetools.func
from eth_utils import ValidationError
import rocksdb

from eth.db.backends.base import (
    BaseAtomicDB,
    BaseDB,
)
from eth.db.diff import (
    DBDiffTracker,
    DiffMissingError,
)


class RocksDB(BaseAtomicDB):
    logger = logging.getLogger("eth.db.backends.RocksDB")

    def __init__(self,
                 db_path: Path = None,
                 opts: 'rocksdb.Options' = None,
                 read_only: bool=False) -> None:
        if not db_path:
            raise TypeError("The RocksDB backend requires a database path")

        if opts is None:
            opts = rocksdb.Options(create_if_missing=True)
        self.db_path = db_path
        self.db = rocksdb.DB(str(db_path), opts, read_only=read_only)

    def __getitem__(self, key: bytes) -> bytes:
        v = self.db.get(key)
        if v is None:
            raise KeyError(key)
        return v

    def __setitem__(self, key: bytes, value: bytes) -> None:
        self.db.put(key, value)

    def _exists(self, key: bytes) -> bool:
        return self.db.get(key) is not None

    def __delitem__(self, key: bytes) -> None:
        exists, _ = self.db.key_may_exist(key)
        if not exists:
            raise KeyError(key)
        self.db.delete(key)

    @contextmanager
    def atomic_batch(self) -> Iterator['RocksDBWriteBatch']:
        batch = rocksdb.WriteBatch()

        readable_batch = RocksDBWriteBatch(self, batch)

        try:
            yield readable_batch
        finally:
            readable_batch.decommission()

        self.db.write(batch)


# A readonly database does only see the data as it was at the point of its creation.
# We assign a TTL to readonly database instances to guarantee that we initiate a
# fresh instance after the TTL has passed.
# A TTL of 100 ms should achieve that the data is reasonable fresh, yet multiple reads
# that are made in one tick do not cause re-initialzing a fresh readonly db over and over.
READ_ONLY_DB_TTL = 1


class ReadonlyRocksDB(BaseAtomicDB):
    logger = logging.getLogger("trinity.db.rocksdb.ReadonlyRocksDB")

    def __init__(self,
                 db_path: Path = None) -> None:
        if not db_path:
            raise TypeError("The RocksDB backend requires a database path")

        self._db_path = db_path

    @cachetools.func.ttl_cache(ttl=READ_ONLY_DB_TTL)
    def _initialize_db(self) -> rocksdb.DB:
        return rocksdb.DB(str(self._db_path), rocksdb.Options(), read_only=True)

    def __getitem__(self, key: bytes) -> bytes:
        db = self._initialize_db()
        v = db.get(key)
        if v is None:
            raise KeyError(key)
        return v

    def __setitem__(self, key: bytes, value: bytes) -> None:
        raise NotImplementedError("Readonly connection")

    def _exists(self, key: bytes) -> bool:
        db = self._initialize_db()
        return db.get(key) is not None

    def __delitem__(self, key: bytes) -> None:
        raise NotImplementedError("Readonly connection")

    def __hash__(self) -> int:
        return hash(self._db_path)

    @contextmanager
    def atomic_batch(self) -> Iterator['RocksDBWriteBatch']:
        raise NotImplementedError("Readonly connection")


class RocksDBWriteBatch(BaseDB):
    """
    A native rocksdb write batch does not permit reads on the in-progress data.
    This class fills that gap, by tracking the in-progress diff, and adding
    a read interface.
    """
    logger = logging.getLogger("eth.db.backends.RocksDBWriteBatch")

    def __init__(self, original_read_db: BaseDB, write_batch: 'rocksdb.WriteBatch') -> None:
        self._original_read_db = original_read_db
        self._write_batch = write_batch
        # keep track of the temporary changes made
        self._track_diff = DBDiffTracker()

    def __getitem__(self, key: bytes) -> bytes:
        if self._track_diff is None:
            raise ValidationError("Cannot get data from a write batch, out of context")

        try:
            changed_value = self._track_diff[key]
        except DiffMissingError as missing:
            if missing.is_deleted:
                raise KeyError(key)
            else:
                return self._original_read_db[key]
        else:
            return changed_value

    def __setitem__(self, key: bytes, value: bytes) -> None:
        if self._track_diff is None:
            raise ValidationError("Cannot set data from a write batch, out of context")

        self._write_batch.put(key, value)
        self._track_diff[key] = value

    def _exists(self, key: bytes) -> bool:
        if self._track_diff is None:
            raise ValidationError("Cannot test data existance from a write batch, out of context")

        try:
            self._track_diff[key]
        except DiffMissingError as missing:
            if missing.is_deleted:
                return False
            else:
                return key in self._original_read_db
        else:
            return True

    def __delitem__(self, key: bytes) -> None:
        if self._track_diff is None:
            raise ValidationError("Cannot delete data from a write batch, out of context")

        self._write_batch.delete(key)
        del self._track_diff[key]

    def decommission(self) -> None:
        """
        Prevent any further actions to be taken on this write batch, called after leaving context
        """
        self._track_diff = None
