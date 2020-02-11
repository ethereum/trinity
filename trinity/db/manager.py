import contextlib
import enum
import errno
import itertools
import logging
import pathlib
import socket
import struct
import threading
from types import TracebackType
from typing import (
    Iterator,
    Type,
)

from eth_utils import ValidationError
from eth_utils.toolz import partition

from eth.abc import (
    AtomicDatabaseAPI,
    DatabaseAPI,
)
from eth.db.backends.base import BaseDB, BaseAtomicDB
from eth.db.diff import DBDiffTracker, DBDiff, DiffMissingError

from trinity._utils.ipc import wait_for_ipc
from trinity._utils.socket import BufferedSocket, IPCSocketServer


@enum.unique
class Operation(enum.Enum):
    GET = b'\x00'
    SET = b'\x01'
    DELETE = b'\x02'
    EXISTS = b'\x03'
    ATOMIC_BATCH = b'\x04'


GET = Operation.GET
"""
GET Request:

- Operation Byte: 0x00
- Key Length: 4-byte little endian
- Key: raw

GET Response (success):

- Success Byte: 0x01
- Value Length: 4-byte little endian
- Value: raw

GET Response (fail):

- Fail Byte: 0x00
"""

SET = Operation.SET
"""
SET Request:

- Operation Byte: 0x01
- Key Length: 4-byte little endian
- Value Length: 4-byte little endian
- Key: raw
- Value: raw

SET Response:

- Success Byte: 0x01
"""

DELETE = Operation.DELETE
"""
DELETE Request:

- Operation Byte: 0x02
- Key Length: 4-byte little endian
- Key: raw

DELETE Response:

- Success Byte: 0x01
"""

EXISTS = Operation.EXISTS
"""
EXISTS Request:

- Operation Byte: 0x03
- Key Length: 4-byte little endian
- Key: raw

EXISTS Response:

- Response Byte: True: 0x01 or False: 0x00
"""
ATOMIC_BATCH = Operation.ATOMIC_BATCH
"""
ATOMIC_BATCH Request:

- Operation Byte: 0x04
- Key/Value Pair Count: 4-byte little endian
- Delete Count: 4-byte little endian
- Key/Value Sizes: Array of 4-byte little endian
- Delete Key Sizes: Array of 4-byte little endian
- Key/Values: Array of raw bytes
- Delete Keys: Array of raw bytes

ATOMIC_BATCH Response:

- Success Byte: 0x01
"""


LEN_BYTES = 4
DOUBLE_LEN_BYTES = 2 * LEN_BYTES


SUCCESS_BYTE = b'\x01'
FAIL_BYTE = b'\x00'


@enum.unique
class Result(enum.Enum):
    SUCCESS = SUCCESS_BYTE
    FAIL = FAIL_BYTE


SUCCESS = Result.SUCCESS
FAIL = Result.FAIL


class DBManager(IPCSocketServer):
    """
    Implements an interface for serving the BaseAtomicDB API over a socket.
    """
    logger = logging.getLogger('trinity.db.manager.DBManager')

    def __init__(self, db: AtomicDatabaseAPI):
        """
        The AtomicDatabaseAPI that this wraps must be threadsafe.
        """
        super().__init__()
        self.db = db

    def serve_conn(self, sock: BufferedSocket) -> None:
        while self.is_running:
            try:
                operation_byte = sock.read_exactly(1)
            except OSError as err:
                self.logger.debug("%s: closing client connection: %s", self, err)
                break
            except Exception:
                self.logger.exception("Error reading operation flag")
                break

            try:
                operation = Operation(operation_byte)
            except TypeError:
                self.logger.error("Unrecognized database operation: %s", operation_byte.hex())
                break

            try:
                if operation is GET:
                    self.handle_GET(sock)
                elif operation is SET:
                    self.handle_SET(sock)
                elif operation is DELETE:
                    self.handle_DELETE(sock)
                elif operation is EXISTS:
                    self.handle_EXISTS(sock)
                elif operation is ATOMIC_BATCH:
                    self.handle_ATOMIC_BATCH(sock)
                else:
                    self.logger.error("Got unhandled operation %s", operation)
            except Exception as err:
                self.logger.exception("Unhandled error during operation %s: %s", operation, err)
                raise

    def handle_GET(self, sock: BufferedSocket) -> None:
        key_size_data = sock.read_exactly(LEN_BYTES)
        key = sock.read_exactly(int.from_bytes(key_size_data, 'little'))
        try:
            value = self.db[key]
        except KeyError:
            sock.sendall(FAIL_BYTE)
        else:
            sock.sendall(SUCCESS_BYTE + len(value).to_bytes(LEN_BYTES, 'little') + value)

    def handle_SET(self, sock: BufferedSocket) -> None:
        key_and_value_size_data = sock.read_exactly(DOUBLE_LEN_BYTES)
        key_size, value_size = struct.unpack('<II', key_and_value_size_data)
        combined_size = key_size + value_size
        key_and_value_data = sock.read_exactly(combined_size)
        key = key_and_value_data[:key_size]
        value = key_and_value_data[key_size:]
        self.db[key] = value
        sock.sendall(SUCCESS_BYTE)

    def handle_DELETE(self, sock: BufferedSocket) -> None:
        key_size_data = sock.read_exactly(LEN_BYTES)
        key = sock.read_exactly(int.from_bytes(key_size_data, 'little'))
        try:
            del self.db[key]
        except KeyError:
            sock.sendall(FAIL_BYTE)
        else:
            sock.sendall(SUCCESS_BYTE)

    def handle_EXISTS(self, sock: BufferedSocket) -> None:
        key_size_data = sock.read_exactly(LEN_BYTES)
        key = sock.read_exactly(int.from_bytes(key_size_data, 'little'))
        if key in self.db:
            sock.sendall(SUCCESS_BYTE)
        else:
            sock.sendall(FAIL_BYTE)

    def handle_ATOMIC_BATCH(self, sock: BufferedSocket) -> None:
        kv_pair_and_delete_count_data = sock.read_exactly(DOUBLE_LEN_BYTES)
        kv_pair_count, delete_count = struct.unpack('<II', kv_pair_and_delete_count_data)
        total_kv_count = 2 * kv_pair_count

        if kv_pair_count or delete_count:
            kv_and_delete_sizes_data = sock.read_exactly(
                DOUBLE_LEN_BYTES * kv_pair_count + LEN_BYTES * delete_count
            )
            fmt_str = '<' + 'I' * (total_kv_count + delete_count)
            kv_and_delete_sizes = struct.unpack(fmt_str, kv_and_delete_sizes_data)

            kv_sizes = kv_and_delete_sizes[:total_kv_count]
            delete_sizes = kv_and_delete_sizes[total_kv_count:total_kv_count + delete_count]

            with self.db.atomic_batch() as batch:
                for key_size, value_size in partition(2, kv_sizes):
                    combined_size = key_size + value_size
                    key_and_value_data = sock.read_exactly(combined_size)
                    key = key_and_value_data[:key_size]
                    value = key_and_value_data[key_size:]
                    batch[key] = value
                for key_size in delete_sizes:
                    key = sock.read_exactly(key_size)
                    del batch[key]

        sock.sendall(SUCCESS_BYTE)


class DBClient(BaseAtomicDB):
    logger = logging.getLogger('trinity.db.client.DBClient')

    def __init__(self, sock: socket.socket):
        self._socket = BufferedSocket(sock)
        self._lock = threading.Lock()

    def __enter__(self) -> None:
        self._socket.__enter__()

    def __exit__(self,
                 exc_type: Type[BaseException],
                 exc_value: BaseException,
                 exc_tb: TracebackType) -> None:
        self._socket.__exit__(exc_type, exc_value, exc_tb)

    def __getitem__(self, key: bytes) -> bytes:
        with self._lock:
            self._socket.sendall(GET.value + len(key).to_bytes(LEN_BYTES, 'little') + key)
            result_byte = self._socket.read_exactly(1)

            if result_byte == SUCCESS_BYTE:
                value_size_data = self._socket.read_exactly(LEN_BYTES)
                value = self._socket.read_exactly(int.from_bytes(value_size_data, 'little'))
                return value
            elif result_byte == FAIL_BYTE:
                raise KeyError(key)
            else:
                raise Exception(f"Unknown result byte: {result_byte.hex}")

    def __setitem__(self, key: bytes, value: bytes) -> None:
        with self._lock:
            self._socket.sendall(
                SET.value + struct.pack('<II', len(key), len(value)) + key + value
            )
            Result(self._socket.read_exactly(1))

    def __delitem__(self, key: bytes) -> None:
        with self._lock:
            self._socket.sendall(DELETE.value + len(key).to_bytes(4, 'little') + key)
            result_byte = self._socket.read_exactly(1)

        if result_byte == SUCCESS_BYTE:
            return
        elif result_byte == FAIL_BYTE:
            raise KeyError(key)
        else:
            raise Exception(f"Unknown result byte: {result_byte.hex}")

    def _exists(self, key: bytes) -> bool:
        with self._lock:
            self._socket.sendall(EXISTS.value + len(key).to_bytes(4, 'little') + key)
            result_byte = self._socket.read_exactly(1)

        if result_byte == SUCCESS_BYTE:
            return True
        elif result_byte == FAIL_BYTE:
            return False
        else:
            raise Exception(f"Unknown result byte: {result_byte.hex}")

    @contextlib.contextmanager
    def atomic_batch(self) -> Iterator['AtomicBatch']:
        batch = AtomicBatch(self)
        yield batch
        diff = batch.finalize()
        pending_deletes = diff.deleted_keys()
        pending_kv_pairs = diff.pending_items()

        kv_pair_count = len(pending_kv_pairs)
        delete_count = len(pending_deletes)

        kv_sizes = tuple(len(item) for item in itertools.chain(*pending_kv_pairs))
        delete_sizes = tuple(len(key) for key in pending_deletes)

        # We encode all of the *sizes* in one shot using `struct.pack` and this
        # dynamically constructed format string.
        fmt_str = '<II' + 'I' * (len(kv_sizes) + len(pending_deletes))
        kv_pair_count_and_size_data = struct.pack(
            fmt_str,
            kv_pair_count,
            delete_count,
            *kv_sizes,
            *delete_sizes,
        )
        kv_and_delete_data = b''.join(itertools.chain(*pending_kv_pairs, pending_deletes))
        with self._lock:
            self._socket.sendall(
                ATOMIC_BATCH.value + kv_pair_count_and_size_data + kv_and_delete_data
            )
            Result(self._socket.read_exactly(1))

    def close(self) -> None:
        try:
            self._socket.shutdown(socket.SHUT_WR)
        except OSError as e:
            # on mac OS this can result in the following error:
            # OSError: [Errno 57] Socket is not connected
            if e.errno != errno.ENOTCONN:
                raise
        self._socket.close()

    @classmethod
    def connect(cls, path: pathlib.Path, timeout: int = 5) -> "DBClient":
        wait_for_ipc(path, timeout)
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        cls.logger.debug("Opened connection to %s: %s", path, s)
        s.connect(str(path))
        return cls(s)


class AtomicBatch(BaseDB):
    """
    This is returned by a DBClient during an atomic_batch, to provide a temporary view
    of the database, before commit.
    """
    logger = logging.getLogger("trinity.db.manager.AtomicBatch")

    _write_target_db: BaseDB = None
    _diff: DBDiffTracker = None

    def __init__(self, db: DatabaseAPI) -> None:
        self._db = db
        self._track_diff = DBDiffTracker()

    def __getitem__(self, key: bytes) -> bytes:
        if self._track_diff is None:
            raise ValidationError("Cannot get data from a write batch, out of context")

        try:
            value = self._track_diff[key]
        except DiffMissingError as missing:
            if missing.is_deleted:
                raise KeyError(key)
            else:
                return self._db[key]
        else:
            return value

    def __setitem__(self, key: bytes, value: bytes) -> None:
        if self._track_diff is None:
            raise ValidationError("Cannot set data from a write batch, out of context")

        self._track_diff[key] = value

    def __delitem__(self, key: bytes) -> None:
        if key not in self:
            raise KeyError(key)
        del self._track_diff[key]

    def _exists(self, key: bytes) -> bool:
        try:
            self[key]
        except KeyError:
            return False
        else:
            return True

    def finalize(self) -> DBDiff:
        diff = self._track_diff.diff()
        self._track_diff = None
        self._db = None
        return diff


def _run() -> None:
    from eth.db.backends.level import LevelDB
    from eth.db.chain import ChainDB
    from trinity.cli_parser import parser
    from trinity.config import Eth1AppConfig, TrinityConfig
    from trinity.constants import APP_IDENTIFIER_ETH1
    from trinity.initialization import (
        initialize_data_dir,
        is_data_dir_initialized,
        is_database_initialized,
        initialize_database,
        ensure_eth1_dirs,
    )

    # Require a root dir to be specified as we don't want to mess with the default one.
    for action in parser._actions:
        if action.dest == 'trinity_root_dir':
            action.required = True
            break

    args = parser.parse_args()
    # FIXME: Figure out a way to avoid having to set this.
    args.sync_mode = "full"
    logging.basicConfig(
        level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s', datefmt='%H:%M:%S')
    for name, level in args.log_levels.items():
        logging.getLogger(name).setLevel(level)
    trinity_config = TrinityConfig.from_parser_args(args, APP_IDENTIFIER_ETH1, (Eth1AppConfig,))
    trinity_config.trinity_root_dir.mkdir(exist_ok=True)
    if not is_data_dir_initialized(trinity_config):
        initialize_data_dir(trinity_config)

    with trinity_config.process_id_file('database'):
        app_config = trinity_config.get_app_config(Eth1AppConfig)
        ensure_eth1_dirs(app_config)

        base_db = LevelDB(db_path=app_config.database_dir)
        chaindb = ChainDB(base_db)

        if not is_database_initialized(chaindb):
            chain_config = app_config.get_chain_config()
            initialize_database(chain_config, chaindb, base_db)

        manager = DBManager(base_db)
        with manager.run(trinity_config.database_ipc_path):
            try:
                manager.wait_stopped()
            except KeyboardInterrupt:
                pass


if __name__ == "__main__":
    _run()
