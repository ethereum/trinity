from typing import (
    Sequence,
    Union,
    Dict,
)
from eth_typing import HexStr
from typing_extensions import TypedDict


class RpcProtocolResponse(TypedDict):
    version: str
    difficulty: int
    genesis: str
    head: str
    network: int
    config: Dict[str, int]


class RpcPortsResponse(TypedDict):
    discovery: int
    listener: int


class RpcNodeInfoResponse(TypedDict):
    enode: str
    ip: str
    listenAddr: str
    name: str
    ports: RpcPortsResponse
    protocols: Dict[str, RpcProtocolResponse]


RpcTransactionResponse = TypedDict('RpcTransactionResponse', {
    'hash': HexStr,
    'nonce': str,
    'gas': str,
    'gasPrice': str,
    # `from` being a reserved word forces us to use the alternate syntax for this type
    'from': HexStr,
    'to': HexStr,
    'value': str,
    'input': HexStr,
    'r': str,
    's': str,
    'v': str,
})


class RpcBlockTransactionResponse(RpcTransactionResponse):
    blockNumber: str
    blockHash: HexStr


class RpcHeaderResponse(TypedDict):
    difficulty: str
    extraData: HexStr
    gasLimit: str
    gasUsed: str
    hash: HexStr
    logsBloom: str
    mixHash: HexStr
    nonce: HexStr
    number: str
    parentHash: HexStr
    receiptsRoot: HexStr
    sha3Uncles: HexStr
    stateRoot: HexStr
    timestamp: str
    transactionsRoot: HexStr
    miner: HexStr


class RpcBlockResponse(TypedDict):
    difficulty: HexStr
    extraData: HexStr
    gasLimit: HexStr
    gasUsed: HexStr
    hash: HexStr
    logsBloom: HexStr
    mixHash: HexStr
    nonce: HexStr
    number: HexStr
    parentHash: HexStr
    receiptsRoot: HexStr
    sha3Uncles: HexStr
    stateRoot: HexStr
    timestamp: HexStr
    transactionsRoot: HexStr
    miner: HexStr
    totalDifficulty: str
    uncles: Sequence[HexStr]
    size: str
    transactions: Union[Sequence[str], Sequence[RpcBlockTransactionResponse]]


class RpcLogResponse(TypedDict):
    address: HexStr
    data: HexStr
    blockHash: HexStr
    blockNumber: str
    logIndex: str
    removed: bool
    topics: Sequence[HexStr]
    transactionHash: HexStr
    transactionIndex: str


RpcReceiptResponse = TypedDict('RpcReceiptResponse', {
    'blockHash': HexStr,
    'blockNumber': str,
    'contractAddress': HexStr,
    'cumulativeGasUsed': str,
    # `from` being a reserved word forces us to use the alternate syntax for this type
    'from': HexStr,
    'gasUsed': str,
    'logs': Sequence[RpcLogResponse],
    'logsBloom': str,
    'root': HexStr,
    'to': HexStr,
    'transactionHash': HexStr,
    'transactionIndex': str,
})
