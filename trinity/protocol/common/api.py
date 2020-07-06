from typing import Union
from cached_property import cached_property

from eth_typing import BlockNumber, Hash32

from p2p.abc import ConnectionAPI
from p2p.logic import Application
from p2p.qualifiers import HasProtocol

from trinity.protocol.eth.api import ETHV63API, ETHV65API, ETHV64API
from trinity.protocol.eth.proto import ETHProtocolV63, ETHProtocolV64, ETHProtocolV65
from trinity.protocol.les.api import LESV1API, LESV2API
from trinity.protocol.les.proto import LESProtocolV1, LESProtocolV2

from .abc import ChainInfoAPI, HeadInfoAPI

AnyETHLES = HasProtocol(ETHProtocolV65) | HasProtocol(ETHProtocolV64) | HasProtocol(
    ETHProtocolV63) | HasProtocol(LESProtocolV2) | HasProtocol(LESProtocolV1)

AnyETHLESAPI = Union[ETHV65API, ETHV64API, ETHV63API, LESV1API, LESV2API]


def choose_eth_or_les_api(
        connection: ConnectionAPI) -> AnyETHLESAPI:

    if connection.has_protocol(ETHProtocolV65):
        return connection.get_logic(ETHV65API.name, ETHV65API)
    elif connection.has_protocol(ETHProtocolV64):
        return connection.get_logic(ETHV64API.name, ETHV64API)
    elif connection.has_protocol(ETHProtocolV63):
        return connection.get_logic(ETHV63API.name, ETHV63API)
    elif connection.has_protocol(LESProtocolV2):
        return connection.get_logic(LESV2API.name, LESV2API)
    elif connection.has_protocol(LESProtocolV1):
        return connection.get_logic(LESV1API.name, LESV1API)
    else:
        raise Exception("Unreachable code path")


class ChainInfo(Application, ChainInfoAPI):
    name = 'eth1-chain-info'

    qualifier = AnyETHLES

    def post_apply(self) -> None:
        logic = choose_eth_or_les_api(self.connection)
        # These things don't change so by storing them as instance variables we free our callsites
        # from having to deal with PeerConnectionLost errors, which can be raised by
        # choose_eth_or_les_api().
        self.network_id = logic.network_id
        self.genesis_hash = logic.genesis_hash


class HeadInfo(Application, HeadInfoAPI):
    name = 'eth1-head-info'

    qualifier = AnyETHLES

    @cached_property
    def _tracker(self) -> HeadInfoAPI:
        api = choose_eth_or_les_api(self.connection)
        return api.head_info

    @property
    def head_td(self) -> int:
        return self._tracker.head_td

    @property
    def head_hash(self) -> Hash32:
        return self._tracker.head_hash

    @property
    def head_number(self) -> BlockNumber:
        return self._tracker.head_number
