import logging

from async_service.base import Service

from validator_client.beacon_node import MockBeaconNode as BeaconNode
from validator_client.clock import Clock
from validator_client.context import Context
from validator_client.duty_executor import duty_executor
from validator_client.duty_scheduler import DutyScheduler
from validator_client.duty_store import DutyStore
from validator_client.publisher import publisher
from validator_client.signatory import Signatory
from validator_client.streams.tee import Tee

logger = logging.getLogger("validator_client.client")
logger.setLevel(logging.DEBUG)


class Client(Service):
    def __init__(self, context: Context) -> None:
        self._context = context

    async def run(self) -> None:
        logger.info("booting client from the provided context...")

        clock = Clock(self._context)
        clock_tee = Tee(clock.stream_ticks())
        duty_store = DutyStore(self._context)
        beacon_node = BeaconNode(self._context)

        duty_scheduler = DutyScheduler(
            self._context, await clock_tee.clone(), duty_store, beacon_node
        )
        duty_provider = duty_executor(
            self._context, await clock_tee.clone(), duty_store, beacon_node
        )
        signatory = Signatory(self._context, duty_provider)

        try:
            self.manager.run_daemon_task(clock_tee.run)
            # TODO: ``publisher`` should run as a daemon task
            # see: https://github.com/ethereum/async-service/issues/51
            self.manager.run_task(publisher, self._context, signatory, beacon_node)
            ds = self.manager.run_child_service(duty_scheduler)
            ss = self.manager.run_child_service(signatory)

            await ds.wait_finished()
            await ss.wait_finished()
            await self.manager.wait_stopping()
        finally:
            logger.info("shutting down client...")
