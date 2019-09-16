from typing import Tuple
from p2p.abc import BehaviorAPI

from p2p.behaviors import Application

from .exchanges import (
    GetBlockBodiesExchange,
    GetBlockHeadersExchange,
    GetNodeDataExchange,
    GetReceiptsExchange,
)


class ETHAPI(Application):
    get_block_bodies: GetBlockBodiesExchange
    get_block_headers: GetBlockHeadersExchange
    get_node_data: GetNodeDataExchange
    get_receipts: GetReceiptsExchange

    def __init__(self):
        self.get_block_bodies = GetBlockBodiesExchange()
        self.register(self.get_block_bodies)

        self.get_block_headers = GetBlockHeadersExchange()
        self.register(self.get_block_headers)

        self.get_node_data = GetNodeDataExchange()
        self.register(self.get_node_data)

        self.get_receipts = GetReceiptsExchange()
        self.register(self.get_receipts)

    def get_behaviors(self) -> Tuple[BehaviorAPI, ...]:
        return (
            self.get_block_bodies,
            self.get_block_headers,
            self.get_node_data,
            self.get_receipts,
        )
