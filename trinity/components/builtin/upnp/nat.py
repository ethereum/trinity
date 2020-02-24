import asyncio
from concurrent.futures import ThreadPoolExecutor
import ipaddress
import netifaces
from typing import (
    AsyncGenerator,
)
from urllib.parse import urlparse

import upnpclient

from lahja import EndpointAPI

from async_service import Service

from eth_utils import get_extended_debug_logger

from p2p.exceptions import (
    NoInternalAddressMatchesDevice,
)

from trinity.components.builtin.upnp.events import NewUPnPMapping


# UPnP discovery can take a long time, so use a loooong timeout here.
UPNP_DISCOVER_TIMEOUT_SECONDS = 30


def find_internal_ip_on_device_network(upnp_dev: upnpclient.upnp.Device) -> str:
    """
    For a given UPnP device, return the internal IP address of this host machine that can
    be used for a NAT mapping.
    """
    parsed_url = urlparse(upnp_dev.location)
    # Get an ipaddress.IPv4Network instance for the upnp device's network.
    upnp_dev_net = ipaddress.ip_network(parsed_url.hostname + '/24', strict=False)
    for iface in netifaces.interfaces():
        for family, addresses in netifaces.ifaddresses(iface).items():
            # TODO: Support IPv6 addresses as well.
            if family != netifaces.AF_INET:
                continue
            for item in addresses:
                if ipaddress.ip_address(item['addr']) in upnp_dev_net:
                    return item['addr']
    raise NoInternalAddressMatchesDevice(device_hostname=parsed_url.hostname)


class UPnPService(Service):
    """
    Generate a mapping of external network IP address/port to internal IP address/port,
    using the Universal Plug 'n' Play standard.
    """
    logger = get_extended_debug_logger('trinity.components.upnp.UPnPService')

    _nat_portmap_lifetime = 30 * 60

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
            try:
                external_ip = await self.add_nat_portmap()
                if external_ip is not None:
                    event = NewUPnPMapping(external_ip)
                    self.logger.debug(
                        "NAT portmap created, broadcasting NewUPnPMapping event: %s", event)
                    await self.event_bus.broadcast(event)
                else:
                    self.logger.info("Unable to setup NAT portmap")
                # Wait for the port mapping lifetime, and then try registering it again
                await asyncio.sleep(self._nat_portmap_lifetime)
            except asyncio.CancelledError:
                # Re-raise to prevent it being logged as an unexpected exception.
                raise
            except Exception:
                self.logger.exception("Failed to setup NAT portmap")

    async def add_nat_portmap(self) -> str:
        """
        Set up the port mapping

        :return: the IP address of the new mapping (or None if failed)
        """
        self.logger.info("Setting up NAT portmap...")
        try:
            async for upnp_dev in self._discover_upnp_devices():
                try:
                    external_ip = await self._add_nat_portmap(upnp_dev)
                except NoInternalAddressMatchesDevice as exc:
                    self.logger.info(
                        "No internal addresses were managed by the UPnP device at %s",
                        exc.device_hostname,
                    )
                    continue
                else:
                    return external_ip
        except upnpclient.soap.SOAPError as e:
            if e.args == (718, 'ConflictInMappingEntry'):
                # An entry already exists with the parameters we specified. Maybe the router
                # didn't clean it up after it expired or it has been configured by other piece
                # of software, either way we should not override it.
                # https://tools.ietf.org/id/draft-ietf-pcp-upnp-igd-interworking-07.html#errors
                self.logger.info("NAT port mapping already configured, not overriding it")
            else:
                self.logger.exception("Failed to setup NAT portmap")

        return None

    async def _add_nat_portmap(self, upnp_dev: upnpclient.upnp.Device) -> str:
        # Detect our internal IP address (which raises if there are no matches)
        internal_ip = find_internal_ip_on_device_network(upnp_dev)

        external_ip = upnp_dev.WANIPConn1.GetExternalIPAddress()['NewExternalIPAddress']
        for protocol, description in [('TCP', 'ethereum p2p'), ('UDP', 'ethereum discovery')]:
            try:
                upnp_dev.WANIPConn1.AddPortMapping(
                    NewRemoteHost=external_ip,
                    NewExternalPort=self.port,
                    NewProtocol=protocol,
                    NewInternalPort=self.port,
                    NewInternalClient=internal_ip,
                    NewEnabled='1',
                    NewPortMappingDescription=description,
                    NewLeaseDuration=self._nat_portmap_lifetime,
                )
            except upnpclient.soap.SOAPError as exc:
                self.logger.warning(
                    "Failed to setup port mapping for %s/%s: %s",
                    protocol,
                    description,
                    exc
                )
            else:
                self.logger.info(
                    "NAT port forwarding successfully set up at %s:%d", external_ip, self.port)
        return external_ip

    async def _discover_upnp_devices(self) -> AsyncGenerator[upnpclient.upnp.Device, None]:
        loop = asyncio.get_event_loop()
        # Use loop.run_in_executor() because upnpclient.discover() is blocking and may take a
        # while to complete. We must use a ThreadPoolExecutor() because the
        # response from upnpclient.discover() can't be pickled.
        with ThreadPoolExecutor() as executor:
            try:
                devices = await asyncio.wait_for(
                    loop.run_in_executor(executor, upnpclient.discover),
                    timeout=UPNP_DISCOVER_TIMEOUT_SECONDS,
                )
            except asyncio.TimeoutError:
                self.logger.info("Timeout waiting for UPNP-enabled devices")
                return
            else:
                self.logger.debug("Found %d candidate NAT devices", len(devices))

        # If there are no UPNP devices we can exit early
        if not devices:
            self.logger.info("No UPNP-enabled devices found")
            return

        # Now we loop over all of the devices until we find one that we can use.
        for device in devices:
            try:
                device.WANIPConn1
            except AttributeError:
                self.logger.debug2("Skipping non UPNP-enabled device: %s", device)
                continue
            yield device
