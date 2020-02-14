from typing import Union
from cached_property import cached_property

from eth_typing import BlockNumber, Hash32

from p2p.abc import ConnectionAPI
from p2p.logic import Application
from p2p.qualifiers import HasProtocol

from trinity.protocol.eth.api import ETHV63API, ETHAPI
from trinity.protocol.eth.proto import ETHProtocolV63, ETHProtocol
from trinity.protocol.les.api import LESV1API, LESV2API
from trinity.protocol.les.proto import LESProtocolV1, LESProtocolV2

from .abc import ChainInfoAPI, HeadInfoAPI

AnyETHLES = HasProtocol(ETHProtocol) | HasProtocol(ETHProtocolV63) | HasProtocol(
    LESProtocolV2) | HasProtocol(LESProtocolV1)


def choose_eth_or_les_api(
        connection: ConnectionAPI) -> Union[ETHAPI, ETHV63API, LESV1API, LESV2API]:

    if connection.has_protocol(ETHProtocol):
        return connection.get_logic(ETHAPI.name, ETHAPI)
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

    @cached_property
    def network_id(self) -> int:
        return self._get_logic().network_id

    @cached_property
    def genesis_hash(self) -> Hash32:
        return self._get_logic().genesis_hash

    def _get_logic(self) -> Union[ETHAPI, ETHV63API, LESV1API, LESV2API]:
        return choose_eth_or_les_api(self.connection)


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
