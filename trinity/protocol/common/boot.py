import asyncio
from typing import cast, TYPE_CHECKING

from eth_utils import ValidationError

from eth.abc import (
    BlockHeaderAPI,
    BlockNumber,
)
from eth.vm.forks import HomesteadVM

from p2p.disconnect import DisconnectReason
from p2p.exceptions import PeerConnectionLost
from p2p.peer import BasePeerBootManager

from trinity.exceptions import DAOForkCheckFailure
from trinity.protocol.eth.forkid import ForkIDHandshakeCheck

from .constants import CHAIN_SPLIT_CHECK_TIMEOUT

if TYPE_CHECKING:
    from .peer import BaseChainPeer  # noqa: F401


class DAOCheckBootManager(BasePeerBootManager):
    peer: 'BaseChainPeer'

    async def run(self) -> None:
        try:
            await self.ensure_same_side_on_dao_fork()
        except DAOForkCheckFailure as err:
            self.logger.debug("DAO fork check with %s failed: %s", self.peer, err)
            self.peer.connection_tracker.record_failure(self.peer.remote, err)
            self.peer.disconnect_nowait(DisconnectReason.USELESS_PEER)

    async def ensure_same_side_on_dao_fork(self) -> None:
        """
        Ensure we're on the same side of the DAO fork as the remote peer.

        If any of our protocol receipts validated the remote's forkid, we take that as
        confirmation that we're on the same side, otherwise we request the remote's DAO fork block
        header and validate it using the VM we have configured for that block.
        """
        for receipt in self.peer.connection.protocol_receipts:
            if receipt.was_check_performed(ForkIDHandshakeCheck):
                self.logger.debug(
                    "Skipping DAO fork block check as ForkID was validated during handshake of %s",
                    receipt.protocol)
                return

        await self.validate_remote_dao_fork_block()

    async def validate_remote_dao_fork_block(self) -> None:
        for start_block, vm_class in self.peer.context.vm_configuration:
            if not issubclass(vm_class, HomesteadVM):
                continue
            elif not vm_class.support_dao_fork:
                break
            elif start_block > vm_class.get_dao_fork_block_number():
                # VM comes after the fork, so stop checking
                break

            dao_fork_num = vm_class.get_dao_fork_block_number()
            start_block = cast(BlockNumber, dao_fork_num - 1)

            try:
                headers = await self.peer.chain_api.get_block_headers(
                    start_block,
                    max_headers=2,
                    reverse=False,
                    timeout=CHAIN_SPLIT_CHECK_TIMEOUT,
                )

            except (asyncio.TimeoutError, PeerConnectionLost) as err:
                raise DAOForkCheckFailure(
                    f"Timed out waiting for DAO fork header from {self.peer}: {err}"
                ) from err
            except ValidationError as err:
                raise DAOForkCheckFailure(
                    f"Invalid header response during DAO fork check: {err}"
                ) from err

            if len(headers) != 2:
                tip_header = await self._get_tip_header()
                if tip_header.block_number < dao_fork_num:
                    self.logger.debug(
                        f"{self.peer} has tip {tip_header!r}, and returned {headers!r} "
                        "at DAO fork #{dao_fork_num}. Peer seems to be syncing..."
                    )
                    return
                else:
                    raise DAOForkCheckFailure(
                        f"{self.peer} has tip {tip_header!r}, but only returned {headers!r} "
                        "at DAO fork #{dao_fork_num}. Peer seems to be witholding DAO headers..."
                    )
            else:
                parent, header = headers

            try:
                vm_class.validate_header(header, parent)
            except ValidationError as err:
                raise DAOForkCheckFailure(f"{self.peer} failed DAO fork check validation: {err}")

    async def _get_tip_header(self) -> BlockHeaderAPI:
        try:
            headers = await self.peer.chain_api.get_block_headers(
                self.peer.head_info.head_hash,
                max_headers=1,
                timeout=CHAIN_SPLIT_CHECK_TIMEOUT,
            )

        except (asyncio.TimeoutError, PeerConnectionLost) as err:
            raise DAOForkCheckFailure(
                f"Timed out waiting for tip header from {self.peer}: {err}"
            ) from err
        except ValidationError as err:
            raise DAOForkCheckFailure(
                f"Invalid header response for tip header during DAO fork check: {err}"
            ) from err
        else:
            if len(headers) != 1:
                raise DAOForkCheckFailure(
                    f"{self.peer} returned {headers!r} when asked for tip"
                )
            else:
                return headers[0]
