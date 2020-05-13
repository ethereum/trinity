from typing import Union

import asks
from eth_typing import BLSPubkey, HexStr


ValidatorPublicKey = Union[HexStr, BLSPubkey, bytes]


class BeaconAPI:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url

    def _mk_url(self, tail_path: str) -> str:
        return f"{self.base_url}{tail_path}"

    async def get_genesis_time(self) -> int:
        # TODO: caching
        response = await asks.get(self._mk_url('/beacon/genesis'))
        data = response.json()
        return int(data['genesis_time'])

    async def get_fork(self) -> None:
        raise NotImplementedError

    async def stream_fork(self) -> None:
        raise NotImplementedError

    async def get_client_version(self) -> str:
        return await asks.get(self._mk_url('/node/version'))

    async def get_syncing(self) -> SyncStatus:
        raw_status = await asks.get(self._mk_url('/node/syncing'))
        # TODO: return a typed object rather than a plain mapping
        return raw_status

    async def get_validator_details(self, public_key: ValidatorPublicKey) -> None:
        raise NotImplementedError

    async def get_attester_duties(self, *publick_keys: ValidatorPublicKey) -> None:
        raise NotImplementedError

    async def submit_attester_duties(self) -> None:
        raise NotImplementedError

    async def get_proposer_duties(self, *publick_keys: ValidatorPublicKey) -> None:
        raise NotImplementedError

    async def submit_proposer_duties(self) -> None:
        raise NotImplementedError

    async def get_committee_subscriptions(self) -> None:
        raise NotImplementedError
