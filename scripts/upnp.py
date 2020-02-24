import argparse
import uuid

import trio

from lahja import ConnectionConfig, TrioEndpoint

from trinity.components.builtin.upnp.events import NewUPnPMapping
from trinity.constants import UPNP_EVENTBUS_ENDPOINT


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--ipc', type=str, help="The path to UPnPService's IPC file")
    args = parser.parse_args()

    connection_config = ConnectionConfig(UPNP_EVENTBUS_ENDPOINT, args.ipc)
    async with TrioEndpoint(f"upnp-watcher-{uuid.uuid4()}").run() as client:
        with trio.fail_after(1):
            await client.connect_to_endpoints(connection_config)

        async for event in client.stream(NewUPnPMapping):
            external_ip = event.ip
            print("Got new UPnP mapping:", external_ip)


if __name__ == "__main__":
    # Connect to a running UPnPService and prints any NewUPnPMapping it broadcasts.
    trio.run(main)
