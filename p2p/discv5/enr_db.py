import binascii
from collections import defaultdict
import logging
import operator
import pathlib
import re
import time
from typing import (
    DefaultDict,
    Dict,
)

from eth_utils import ValidationError

import rlp

import trio

from eth_utils import encode_hex

from p2p.discv5.abc import EnrDbApi
from p2p.discv5.enr import ENR
from p2p.discv5.identity_schemes import IdentitySchemeRegistry
from p2p.discv5.typing import NodeID


ACCEPTABLE_ENR_LOAD_TIME = 1.0


class BaseEnrDb(EnrDbApi):

    def __init__(self, identity_scheme_registry: IdentitySchemeRegistry):
        self.logger = logging.getLogger(".".join((
            self.__module__,
            self.__class__.__name__,
        )))
        self._identity_scheme_registry = identity_scheme_registry

    @property
    def identity_scheme_registry(self) -> IdentitySchemeRegistry:
        return self._identity_scheme_registry

    def validate_identity_scheme(self, enr: ENR) -> None:
        """Check that we know the identity scheme of the ENR.

        This check should be performed whenever an ENR is inserted or updated in serialized form to
        make sure retrieving it at a later time will succeed (deserializing the ENR would fail if
        we don't know the identity scheme).
        """
        if enr.identity_scheme.id not in self.identity_scheme_registry:
            raise ValueError(
                f"ENRs identity scheme with id {enr.identity_scheme.id!r} unknown to ENR DBs "
                f"identity scheme registry"
            )

    async def insert_or_update(self, enr: ENR) -> None:
        try:
            await self.update(enr)
        except KeyError:
            await self.insert(enr)


def get_enr_filename(enr: ENR) -> str:
    return f"enr_{encode_hex(enr.node_id)}_{enr.sequence_number}"


def is_valid_enr_filename(filename: str) -> bool:
    return bool(re.match(r"^enr_0x[0-9a-f]{64}_\d+$", filename))


class FileEnrDb(BaseEnrDb):
    def __init__(self,
                 identity_scheme_registry: IdentitySchemeRegistry,
                 directory: pathlib.Path) -> None:
        super().__init__(identity_scheme_registry)
        self.directory = directory
        self.enrs: DefaultDict[NodeID, Dict[int, ENR]] = defaultdict(dict)
        self.load_enrs_timed()

    def load_enrs_timed(self) -> None:
        start_time = time.time()

        self.load_enrs()

        end_time = time.time()
        total_time = end_time - start_time

        if total_time > ACCEPTABLE_ENR_LOAD_TIME:
            self.logger.warning("Loading ENRs took a very long time: %.1f seconds", total_time)
        else:
            self.logger.debug("Loading ENRs took %.1f seconds", total_time)

    def load_enrs(self) -> None:
        enr_paths = tuple(
            path for path in self.directory.iterdir()
            if path.is_file()
        )

        invalid_enr_paths = tuple(
            path for path in enr_paths
            if not is_valid_enr_filename(path.name)
        )
        for invalid_enr_path in invalid_enr_paths:
            self.logger.warning(
                "Encountered invalid ENR filename %s in ENR directory %s",
                invalid_enr_path,
                self.directory
            )

        valid_enr_paths = tuple(
            path for path in enr_paths
            if path not in invalid_enr_paths
        )
        for path in valid_enr_paths:
            try:
                enr = self.load_enr_file(path)
            except (binascii.Error, rlp.DeserializationError, ValidationError) as error:
                self.logger.warning("Encountered invalid ENR in file %s: %s", path, error)
                continue

            if get_enr_filename(enr) != path.name:
                self.logger.warning(
                    "ENR in %s has inconsistent name (actual node id: %s, actual seq num: %d)",
                    path,
                    encode_hex(enr.node_id),
                    enr.sequence_number
                )

            self.enrs[enr.node_id][enr.sequence_number] = enr

    def load_enr_file(self, path: pathlib.Path) -> ENR:
        enr_base64 = path.read_text()
        enr = ENR.from_repr(enr_base64, self.identity_scheme_registry)
        return enr

    def write_enr_file(self, enr: ENR) -> None:
        path = self.directory / get_enr_filename(enr)
        path.write_text(repr(enr))

    async def insert(self, enr: ENR) -> None:
        self.validate_identity_scheme(enr)

        if await self.contains(enr.node_id):
            raise ValueError("ENR with node id %s already exists", encode_hex(enr.node_id))
        else:
            self.logger.debug(
                "Inserting new ENR of %s with sequence number %d",
                encode_hex(enr.node_id),
                enr.sequence_number,
            )
            self.enrs[enr.node_id][enr.sequence_number] = enr
            self.write_enr_file(enr)

    async def update(self, enr: ENR) -> None:
        self.validate_identity_scheme(enr)

        existing_enr = await self.get(enr.node_id)
        if existing_enr.sequence_number < enr.sequence_number:
            self.logger.debug(
                "Updating ENR of %s from sequence number %d to %d",
                encode_hex(enr.node_id),
                existing_enr.sequence_number,
                enr.sequence_number,
            )
            self.enrs[enr.node_id][enr.sequence_number] = enr
            self.write_enr_file(enr)
        else:
            self.logger.debug(
                "Not updating ENR of %s as new sequence number %d is not higher than the current "
                "one %d",
                encode_hex(enr.node_id),
                enr.sequence_number,
                existing_enr.sequence_number,
            )

    async def get(self, node_id: NodeID) -> ENR:
        await trio.hazmat.checkpoint()
        enrs = self.enrs[node_id]
        if not enrs:
            raise KeyError(f"No ENR for node {encode_hex(node_id)} present in DB")
        else:
            # get enr with highest sequence number
            _, enr = max(enrs.items(), key=operator.itemgetter(0))
            return enr

    async def contains(self, node_id: NodeID) -> bool:
        await trio.hazmat.checkpoint()
        return bool(self.enrs[node_id])

    def __len__(self) -> int:
        return len(self.enrs)


class MemoryEnrDb(BaseEnrDb):

    def __init__(self, identity_scheme_registry: IdentitySchemeRegistry):
        super().__init__(identity_scheme_registry)

        self.key_value_storage: Dict[NodeID, ENR] = {}

    async def insert(self, enr: ENR) -> None:
        self.validate_identity_scheme(enr)

        if await self.contains(enr.node_id):
            raise ValueError("ENR with node id %s already exists", encode_hex(enr.node_id))
        else:
            self.logger.debug(
                "Inserting new ENR of %s with sequence number %d",
                encode_hex(enr.node_id),
                enr.sequence_number,
            )
            self.key_value_storage[enr.node_id] = enr

    async def update(self, enr: ENR) -> None:
        self.validate_identity_scheme(enr)
        existing_enr = await self.get(enr.node_id)
        if existing_enr.sequence_number < enr.sequence_number:
            self.logger.debug(
                "Updating ENR of %s from sequence number %d to %d",
                encode_hex(enr.node_id),
                existing_enr.sequence_number,
                enr.sequence_number,
            )
            self.key_value_storage[enr.node_id] = enr
        else:
            self.logger.debug(
                "Not updating ENR of %s as new sequence number %d is not higher than the current "
                "one %d",
                encode_hex(enr.node_id),
                enr.sequence_number,
                existing_enr.sequence_number,
            )

    async def remove(self, node_id: NodeID) -> None:
        self.key_value_storage.pop(node_id)
        self.logger.debug("Removing ENR of %s", encode_hex(node_id))

        await trio.sleep(0)  # add checkpoint to make this a proper async function

    async def get(self, node_id: NodeID) -> ENR:
        await trio.sleep(0)  # add checkpoint to make this a proper async function
        return self.key_value_storage[node_id]

    async def contains(self, node_id: NodeID) -> bool:
        await trio.sleep(0)  # add checkpoint to make this a proper async function
        return node_id in self.key_value_storage

    def __len__(self) -> int:
        return len(self.key_value_storage)
