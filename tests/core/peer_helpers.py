from cancel_token.token import CancelToken
from trinity.protocol.eth.peer import ETHPeerPool


class MockPeerPoolWithConnectedPeers(ETHPeerPool):
    def __init__(self, peers, event_bus=None) -> None:
        super().__init__(
            privkey=None, context=None, event_bus=event_bus,
            token=CancelToken("MockPeerPoolWithConnectedPeers"))
        for peer in peers:
            self.connected_nodes[peer.session] = peer

    async def _run(self) -> None:
        raise NotImplementedError("This is a mock PeerPool implementation, you must not _run() it")
