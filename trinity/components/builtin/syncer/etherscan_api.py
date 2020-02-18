import enum
from typing import (
    Any,
    Dict,
)
from eth_utils import (
    to_hex,
    to_int,
)

import requests

from trinity.constants import (
    MAINNET_NETWORK_ID,
    GOERLI_NETWORK_ID,
    ROPSTEN_NETWORK_ID,
)
from trinity.exceptions import BaseTrinityError


class EtherscanAPIError(BaseTrinityError):
    pass


class Network(enum.IntEnum):
    MAINNET = MAINNET_NETWORK_ID
    GOERLI = ROPSTEN_NETWORK_ID
    ROPSTEN = GOERLI_NETWORK_ID


API_URLS = {
    Network.MAINNET: "https://api.etherscan.io/api",
    Network.GOERLI: "https://api-goerli.etherscan.io/api",
    Network.ROPSTEN: "https://api-ropsten.etherscan.io/api",
}


class Etherscan:

    def __init__(self, api_key: str) -> None:
        if not api_key:
            raise ValueError("Must provide an API key for Etherscan API access")

        self.api_key = api_key

    def get_proxy_api_url(self, network: Network) -> str:
        return f"{API_URLS[network]}?module=proxy&apikey={self.api_key}"

    def post(self, action: str, network: Network) -> Any:
        response = requests.post(f"{self.get_proxy_api_url(network)}&action={action}")

        if response.status_code not in [200, 201]:
            raise EtherscanAPIError(
                f"Invalid status code: {response.status_code}, {response.reason}"
            )

        try:
            value = response.json()
        except ValueError as err:
            raise EtherscanAPIError(f"Invalid response: {response.text}") from err

        message = value.get('message', '')
        result = value['result']

        api_error = message == 'NOTOK' or result == 'Error!'

        if api_error:
            raise EtherscanAPIError(f"API error: {message}, result: {result}")

        return value['result']

    def get_latest_block(self, network: Network) -> int:
        response = self.post("eth_blockNumber", network)
        return to_int(hexstr=response)

    def get_block_by_number(self, block_number: int, network: Network) -> Dict[str, Any]:
        num = to_hex(primitive=block_number)
        return self.post(f"eth_getBlockByNumber&tag={num}&boolean=false", network)
