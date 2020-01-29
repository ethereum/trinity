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

from p2p.abc import NodeAPI
from p2p.discv5.abc import NodeDBAPI
from p2p.discv5.enr import ENR
from p2p.discv5.identity_schemes import IdentitySchemeRegistry
from p2p.discv5.typing import NodeID
from p2p.kademlia import Node


ACCEPTABLE_LOAD_TIME = 1.0


class BaseNodeDB(NodeDBAPI):

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

    async def insert_or_update(self, node: NodeAPI) -> None:
        try:
            await self.update(node)
        except KeyError:
            await self.insert(node)


def get_node_filename(node: NodeAPI) -> str:
    return f"node_{encode_hex(node.id)}_{node.enr.sequence_number}"


def is_valid_node_filename(filename: str) -> bool:
    return bool(re.match(r"^node_0x[0-9a-f]{64}_\d+$", filename))


class FileNodeDB(BaseNodeDB):
    def __init__(self,
                 identity_scheme_registry: IdentitySchemeRegistry,
                 directory: pathlib.Path) -> None:
        super().__init__(identity_scheme_registry)
        self.directory = directory
        self.nodes: DefaultDict[NodeID, Dict[int, NodeAPI]] = defaultdict(dict)
        self.load_nodes_timed()

    def load_nodes_timed(self) -> None:
        start_time = time.time()

        self.load_nodes()

        end_time = time.time()
        total_time = end_time - start_time

        if total_time > ACCEPTABLE_LOAD_TIME:
            self.logger.warning("Loading nodes took a very long time: %.1f seconds", total_time)
        else:
            self.logger.debug("Loading nodes took %.1f seconds", total_time)

    def load_nodes(self) -> None:
        node_paths = tuple(
            path for path in self.directory.iterdir()
            if path.is_file()
        )

        invalid_paths = tuple(
            path for path in node_paths
            if not is_valid_node_filename(path.name)
        )
        for invalid_path in invalid_paths:
            self.logger.warning(
                "Encountered invalid Node filename %s in NodeDB directory %s",
                invalid_path,
                self.directory
            )

        valid_paths = tuple(
            path for path in node_paths
            if path not in invalid_paths
        )
        for path in valid_paths:
            try:
                node = self.load_file(path)
            except (
                binascii.Error,
                rlp.DeserializationError,
                ValidationError,
            ) as error:
                self.logger.warning("Encountered invalid Node in file %s: %s", path, error)
                continue

            if get_node_filename(node) != path.name:
                self.logger.warning(
                    "Node in %s has inconsistent name (actual node id: %s, actual seq num: %d)",
                    path,
                    encode_hex(node.id),
                    node.enr.sequence_number
                )

            self.nodes[node.id][node.enr.sequence_number] = node

    def load_file(self, path: pathlib.Path) -> NodeAPI:
        enr_base64 = path.read_text()
        enr = ENR.from_repr(enr_base64, self.identity_scheme_registry)
        return Node(enr)

    def write_file(self, node: NodeAPI) -> None:
        path = self.directory / get_node_filename(node)
        path.write_text(repr(node.enr))

    async def insert(self, node: NodeAPI) -> None:
        self.validate_identity_scheme(node.enr)

        if await self.contains(node.id):
            raise ValueError("Node with node id %s already exists", encode_hex(node.id))
        else:
            self.logger.debug(
                "Inserting new Node of %s with sequence number %d",
                encode_hex(node.id),
                node.enr.sequence_number,
            )
            self.nodes[node.id][node.enr.sequence_number] = node
            self.write_file(node)

    async def update(self, node: NodeAPI) -> None:
        self.validate_identity_scheme(node.enr)

        existing_node = await self.get(node.id)
        if existing_node.enr.sequence_number < node.enr.sequence_number:
            self.logger.debug(
                "Updating Node of %s from sequence number %d to %d",
                encode_hex(node.id),
                existing_node.enr.sequence_number,
                node.enr.sequence_number,
            )
            self.nodes[node.id][node.enr.sequence_number] = node
            self.write_file(node)
        else:
            self.logger.debug(
                "Not updating Node of %s as new sequence number %d is not higher than the current "
                "one %d",
                encode_hex(node.id),
                node.enr.sequence_number,
                existing_node.enr.sequence_number,
            )

    async def get(self, node_id: NodeID) -> NodeAPI:
        await trio.hazmat.checkpoint()
        nodes = self.nodes[node_id]
        if not nodes:
            raise KeyError(f"No Node for {encode_hex(node_id)} present in DB")
        else:
            # get Node with highest sequence number
            _, node = max(nodes.items(), key=operator.itemgetter(0))
            return node

    async def contains(self, node_id: NodeID) -> bool:
        await trio.hazmat.checkpoint()
        return bool(self.nodes[node_id])

    def __len__(self) -> int:
        return len(self.nodes)


class MemoryNodeDB(BaseNodeDB):

    def __init__(self, identity_scheme_registry: IdentitySchemeRegistry):
        super().__init__(identity_scheme_registry)

        self.key_value_storage: Dict[NodeID, NodeAPI] = {}

    async def insert(self, node: NodeAPI) -> None:
        self.validate_identity_scheme(node.enr)

        if await self.contains(node.id):
            raise ValueError("Node with id %s already exists", encode_hex(node.id))
        else:
            self.logger.debug(
                "Inserting new Node of %s with sequence number %d",
                encode_hex(node.id),
                node.enr.sequence_number,
            )
            self.key_value_storage[node.id] = node

    async def update(self, node: NodeAPI) -> None:
        self.validate_identity_scheme(node.enr)
        existing_node = await self.get(node.id)
        if existing_node.enr.sequence_number < node.enr.sequence_number:
            self.logger.debug(
                "Updating Node of %s from sequence number %d to %d",
                encode_hex(node.id),
                existing_node.enr.sequence_number,
                node.enr.sequence_number,
            )
            self.key_value_storage[node.id] = node
        else:
            self.logger.debug(
                "Not updating Node of %s as new sequence number %d is not higher than the current "
                "one %d",
                encode_hex(node.id),
                node.enr.sequence_number,
                existing_node.enr.sequence_number,
            )

    async def remove(self, node_id: NodeID) -> None:
        self.key_value_storage.pop(node_id)
        self.logger.debug("Removing Node of %s", encode_hex(node_id))

        await trio.sleep(0)  # add checkpoint to make this a proper async function

    async def get(self, node_id: NodeID) -> NodeAPI:
        await trio.sleep(0)  # add checkpoint to make this a proper async function
        return self.key_value_storage[node_id]

    async def contains(self, node_id: NodeID) -> bool:
        await trio.sleep(0)  # add checkpoint to make this a proper async function
        return node_id in self.key_value_storage

    def __len__(self) -> int:
        return len(self.key_value_storage)
