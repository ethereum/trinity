import asyncio
from argparse import (
    ArgumentParser,
    _SubParsersAction,
)
from trinity.endpoint import TrinityEventBusEndpoint
from trinity.extensibility import BaseIsolatedPlugin


class ValidatorPlugin(BaseIsolatedPlugin):

    @property
    def name(self) -> str:
        return "Validator"

    def configure_parser(self, arg_parser: ArgumentParser, subparser: _SubParsersAction) -> None:
        arg_parser.add_argument(
            "--validator",
            action="store_true",
            help="Run as the validator node",
        )

    def on_ready(self, manager_eventbus: TrinityEventBusEndpoint) -> None:
        args = self.context.args
        if args.validator:
            self.start()

    def do_start(self) -> None:
        print("fooooooooooooooo")
        loop = asyncio.get_event_loop()
        # trinity_config = self.context.trinity_config

        # TODO:
        #   - Add a `ValidatorService` that print something every N seconds.
        #   - Get access to data/event from `BeaconNode`, through `EventBus` or other ways.
        #   - The service broadcasts a block through `BeaconNode` every N seconds.
        #   - The service broadcasts a block if it finds itself selected as a proposer.
        loop.run_forever()
        loop.close()
