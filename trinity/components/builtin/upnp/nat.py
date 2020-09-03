from lahja import EndpointAPI
import trio

from async_service import Service
from upnp_port_forward import setup_port_map, PortMapFailed

from p2p.trio_utils import every

from trinity.components.builtin.upnp.events import UPnPMapping
from trinity.constants import FIRE_AND_FORGET_BROADCASTING
from trinity._utils.logging import get_logger


# UPnP discovery can take a long time, so use a loooong timeout here.
UPNP_DISCOVER_TIMEOUT_SECONDS = 30
UPNP_PORTMAP_DURATION = 30 * 60  # 30 minutes


logger = get_logger('trinity.components.upnp')


class UPnPService(Service):
    logger = get_logger('trinity.components.upnp.UPnPService')

    def __init__(self, port: int, event_bus: EndpointAPI) -> None:
        """
        :param port: The port that a server wants to bind to on this machine, and
        make publicly accessible.
        """
        self.port = port
        self.event_bus = event_bus

    async def run(self) -> None:
        """Run an infinite loop refreshing our NAT port mapping.

        On every iteration we configure the port mapping with a lifetime of 30 minutes and then
        sleep for that long as well.
        """
        while self.manager.is_running:
            async for _ in every(UPNP_PORTMAP_DURATION):
                with trio.move_on_after(UPNP_DISCOVER_TIMEOUT_SECONDS) as scope:
                    try:
                        internal_ip, external_ip = await trio.to_thread.run_sync(
                            setup_port_map,
                            self.port,
                            UPNP_PORTMAP_DURATION,
                        )
                        event = UPnPMapping(str(external_ip))
                        self.logger.debug(
                            "NAT portmap created, broadcasting UPnPMapping event: %s", event)
                        await self.event_bus.broadcast(event, FIRE_AND_FORGET_BROADCASTING)
                    except PortMapFailed as err:
                        self.logger.error("Failed to setup NAT portmap: %s", err)
                    except Exception:
                        self.logger.exception("Error setuping NAT portmap")

                if scope.cancelled_caught:
                    self.logger.error("Timeout attempting to setup UPnP port map")
