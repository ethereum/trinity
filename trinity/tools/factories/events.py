from typing import Sequence, Any

from p2p.abc import SessionAPI

try:
    import factory
except ImportError:
    raise ImportError(
        "The p2p.tools.factories module requires the `factory_boy` and `faker` libraries."
    )


from trinity.protocol.common.events import GetConnectedPeersResponse, PeerInfo


class GetConnectedPeersResponseFactory(factory.Factory):
    class Meta:
        model = GetConnectedPeersResponse

    @classmethod
    def from_sessions(cls,
                      sessions: Sequence[SessionAPI],
                      *args: Any,
                      **kwargs: Any) -> GetConnectedPeersResponse:
        return GetConnectedPeersResponse(tuple(
            PeerInfo(
                session=session,
                capabilities=(),
                client_version_string='unknown',
                inbound=False
            ) for session in sessions
        ))
