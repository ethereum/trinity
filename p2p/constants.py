# DEVP2P Protocol versions
DEVP2P_V4 = 4
DEVP2P_V5 = 5

# Overhead added by ECIES encryption
ENCRYPT_OVERHEAD_LENGTH = 113

# Lentgh of elliptic S256 signatures
SIGNATURE_LEN = 65

# Length of public keys: 512 bit keys in uncompressed form, without format byte
PUBKEY_LEN = 64

# Hash length (for nonce etc)
HASH_LEN = 32

# Length of initial auth handshake message
AUTH_MSG_LEN = SIGNATURE_LEN + HASH_LEN + PUBKEY_LEN + HASH_LEN + 1

# Length of auth ack handshake message
AUTH_ACK_LEN = PUBKEY_LEN + HASH_LEN + 1

# Length of encrypted pre-EIP-8 initiator handshake
ENCRYPTED_AUTH_MSG_LEN = AUTH_MSG_LEN + ENCRYPT_OVERHEAD_LENGTH

# Length of encrypted pre-EIP-8 handshake reply
ENCRYPTED_AUTH_ACK_LEN = AUTH_ACK_LEN + ENCRYPT_OVERHEAD_LENGTH

# Length of an RLPx packet's header
HEADER_LEN = 16

# Length of an RLPx header's/frame's MAC
MAC_LEN = 16

# The amount of seconds a connection can be idle.
CONN_IDLE_TIMEOUT = 30

# The amount of seconds a p2p handshake can take.
HANDSHAKE_TIMEOUT = 10

# Timeout used when waiting for a reply from a remote node.
REPLY_TIMEOUT = 3
MAX_REQUEST_ATTEMPTS = 3

# Default timeout before giving up on a caller-initiated interaction
COMPLETION_TIMEOUT = 5

MAINNET_BOOTNODES = (
    'enode://a979fb575495b8d6db44f750317d0f4622bf4c2aa3365d6af7c284339968eef29b69ad0dce72a4d8db5ebb4968de0e3bec910127f134779fbcb0cb6d3331163c@52.16.188.185:30303',  # noqa: E501
    'enode://aa36fdf33dd030378a0168efe6ed7d5cc587fafa3cdd375854fe735a2e11ea3650ba29644e2db48368c46e1f60e716300ba49396cd63778bf8a818c09bded46f@13.93.211.84:30303',  # noqa: E501
    'enode://78de8a0916848093c73790ead81d1928bec737d565119932b98c6b100d944b7a95e94f847f689fc723399d2e31129d182f7ef3863f2b4c820abbf3ab2722344d@191.235.84.50:30303',  # noqa: E501
    'enode://158f8aab45f6d19c6cbf4a089c2670541a8da11978a2f90dbf6a502a4a3bab80d288afdbeb7ec0ef6d92de563767f3b1ea9e8e334ca711e9f8e2df5a0385e8e6@13.75.154.138:30303',  # noqa: E501
    'enode://1118980bf48b0a3640bdba04e0fe78b1add18e1cd99bf22d53daac1fd9972ad650df52176e7c7d89d1114cfef2bc23a2959aa54998a46afcf7d91809f0855082@52.74.57.123:30303',   # noqa: E501

    # Geth Bootnodes
    # from https://github.com/ethereum/go-ethereum/blob/1bed5afd92c22a5001aff01620671caccd94a6f8/params/bootnodes.go#L22  # noqa: E501
    "enode://d860a01f9722d78051619d1e2351aba3f43f943f6f00718d1b9baa4101932a1f5011f16bb2b1bb35db20d6fe28fa0bf09636d26a87d31de9ec6203eeedb1f666@18.138.108.67:30303",  # noqa: E501
    "enode://22a8232c3abc76a16ae9d6c3b164f98775fe226f0917b0ca871128a74a8e9630b458460865bab457221f1d448dd9791d24c4e5d88786180ac185df813a68d4de@3.209.45.79:30303",  # noqa: E501
    "enode://ca6de62fce278f96aea6ec5a2daadb877e51651247cb96ee310a318def462913b653963c155a0ef6c7d50048bba6e6cea881130857413d9f50a621546b590758@34.255.23.113:30303",  # noqa: E501
    "enode://279944d8dcd428dffaa7436f25ca0ca43ae19e7bcf94a8fb7d1641651f92d121e972ac2e8f381414b80cc8e5555811c2ec6e1a99bb009b3f53c4c69923e11bd8@35.158.244.151:30303",  # noqa: E501
    "enode://8499da03c47d637b20eee24eec3c356c9a2e6148d6fe25ca195c7949ab8ec2c03e3556126b0d7ed644675e78c4318b08691b7b57de10e5f0d40d05b09238fa0a@52.187.207.27:30303",  # noqa: E501
    "enode://103858bdb88756c71f15e9b5e09b56dc1be52f0a5021d46301dbbfb7e130029cc9d0d6f73f693bc29b665770fff7da4d34f3c6379fe12721b5d7a0bcb5ca1fc1@191.234.162.198:30303",  # noqa: E501
    "enode://715171f50508aba88aecd1250af392a45a330af91d7b90701c436b618c86aaa1589c9184561907bebbb56439b8f8787bc01f49a7c77276c58c1b09822d75e8e8@52.231.165.108:30303",  # noqa: E501
    "enode://5d6d7cd20d6da4bb83a1d28cadb5d409b64edf314c0335df658c1a54e32c7c4a7ab7823d57c39b6a757556e68ff1df17c748b698544a55cb488b52479a92b60f@104.42.217.25:30303",  # noqa: E501

    # Parity Bootnodes
    # from https://raw.githubusercontent.com/paritytech/parity-ethereum/master/ethcore/res/ethereum/foundation.json  # noqa: E501
    "enode://81863f47e9bd652585d3f78b4b2ee07b93dad603fd9bc3c293e1244250725998adc88da0cef48f1de89b15ab92b15db8f43dc2b6fb8fbd86a6f217a1dd886701@193.70.55.37:30303",  # noqa: E501
    "enode://4afb3a9137a88267c02651052cf6fb217931b8c78ee058bb86643542a4e2e0a8d24d47d871654e1b78a276c363f3c1bc89254a973b00adc359c9e9a48f140686@144.217.139.5:30303",  # noqa: E501
    "enode://c16d390b32e6eb1c312849fe12601412313165df1a705757d671296f1ac8783c5cff09eab0118ac1f981d7148c85072f0f26407e5c68598f3ad49209fade404d@139.99.51.203:30303",  # noqa: E501
    "enode://4faf867a2e5e740f9b874e7c7355afee58a2d1ace79f7b692f1d553a1134eddbeb5f9210dd14dc1b774a46fd5f063a8bc1fa90579e13d9d18d1f59bac4a4b16b@139.99.160.213:30303",  # noqa: E501
    "enode://6a868ced2dec399c53f730261173638a93a40214cf299ccf4d42a76e3fa54701db410669e8006347a4b3a74fa090bb35af0320e4bc8d04cf5b7f582b1db285f5@163.172.131.191:30303",  # noqa: E501
    "enode://66a483383882a518fcc59db6c017f9cd13c71261f13c8d7e67ed43adbbc82a932d88d2291f59be577e9425181fc08828dc916fdd053af935a9491edf9d6006ba@212.47.247.103:30303",  # noqa: E501
    "enode://cd6611461840543d5b9c56fbf088736154c699c43973b3a1a32390cf27106f87e58a818a606ccb05f3866de95a4fe860786fea71bf891ea95f234480d3022aa3@163.172.157.114:30303",  # noqa: E501
    "enode://1d1f7bcb159d308eb2f3d5e32dc5f8786d714ec696bb2f7e3d982f9bcd04c938c139432f13aadcaf5128304a8005e8606aebf5eebd9ec192a1471c13b5e31d49@138.201.223.35:30303",  # noqa: E501
    "enode://0cc5f5ffb5d9098c8b8c62325f3797f56509bff942704687b6530992ac706e2cb946b90a34f1f19548cd3c7baccbcaea354531e5983c7d1bc0dee16ce4b6440b@40.118.3.223:30305",  # noqa: E501
    "enode://1c7a64d76c0334b0418c004af2f67c50e36a3be60b5e4790bdac0439d21603469a85fad36f2473c9a80eb043ae60936df905fa28f1ff614c3e5dc34f15dcd2dc@40.118.3.223:30308",  # noqa: E501
    "enode://85c85d7143ae8bb96924f2b54f1b3e70d8c4d367af305325d30a61385a432f247d2c75c45c6b4a60335060d072d7f5b35dd1d4c45f76941f62a4f83b6e75daaf@40.118.3.223:30309",  # noqa: E501
    "enode://de471bccee3d042261d52e9bff31458daecc406142b401d4cd848f677479f73104b9fdeb090af9583d3391b7f10cb2ba9e26865dd5fca4fcdc0fb1e3b723c786@54.94.239.50:30303",  # noqa: E501
    "enode://4cd540b2c3292e17cff39922e864094bf8b0741fcc8c5dcea14957e389d7944c70278d872902e3d0345927f621547efa659013c400865485ab4bfa0c6596936f@138.201.144.135:30303",  # noqa: E501
    "enode://01f76fa0561eca2b9a7e224378dd854278735f1449793c46ad0c4e79e8775d080c21dcc455be391e90a98153c3b05dcc8935c8440de7b56fe6d67251e33f4e3c@51.15.42.252:30303",  # noqa: E501
    "enode://2c9059f05c352b29d559192fe6bca272d965c9f2290632a2cfda7f83da7d2634f3ec45ae3a72c54dd4204926fb8082dcf9686e0d7504257541c86fc8569bcf4b@163.172.171.38:30303",  # noqa: E501
    "enode://efe4f2493f4aff2d641b1db8366b96ddacfe13e7a6e9c8f8f8cf49f9cdba0fdf3258d8c8f8d0c5db529f8123c8f1d95f36d54d590ca1bb366a5818b9a4ba521c@163.172.187.252:30303",  # noqa: E501
    "enode://bcc7240543fe2cf86f5e9093d05753dd83343f8fda7bf0e833f65985c73afccf8f981301e13ef49c4804491eab043647374df1c4adf85766af88a624ecc3330e@136.243.154.244:30303",  # noqa: E501
    "enode://ed4227681ca8c70beb2277b9e870353a9693f12e7c548c35df6bca6a956934d6f659999c2decb31f75ce217822eefca149ace914f1cbe461ed5a2ebaf9501455@88.212.206.70:30303",  # noqa: E501
    "enode://cadc6e573b6bc2a9128f2f635ac0db3353e360b56deef239e9be7e7fce039502e0ec670b595f6288c0d2116812516ad6b6ff8d5728ff45eba176989e40dead1e@37.128.191.230:30303",  # noqa: E501
    "enode://595a9a06f8b9bc9835c8723b6a82105aea5d55c66b029b6d44f229d6d135ac3ecdd3e9309360a961ea39d7bee7bac5d03564077a4e08823acc723370aace65ec@46.20.235.22:30303",  # noqa: E501
    "enode://029178d6d6f9f8026fc0bc17d5d1401aac76ec9d86633bba2320b5eed7b312980c0a210b74b20c4f9a8b0b2bf884b111fa9ea5c5f916bb9bbc0e0c8640a0f56c@216.158.85.185:30303",  # noqa: E501
    "enode://fdd1b9bb613cfbc200bba17ce199a9490edc752a833f88d4134bf52bb0d858aa5524cb3ec9366c7a4ef4637754b8b15b5dc913e4ed9fdb6022f7512d7b63f181@212.47.247.103:30303",  # noqa: E501
    "enode://cc26c9671dffd3ee8388a7c8c5b601ae9fe75fc0a85cedb72d2dd733d5916fad1d4f0dcbebad5f9518b39cc1f96ba214ab36a7fa5103aaf17294af92a89f227b@52.79.241.155:30303",  # noqa: E501
    "enode://140872ce4eee37177fbb7a3c3aa4aaebe3f30bdbf814dd112f6c364fc2e325ba2b6a942f7296677adcdf753c33170cb4999d2573b5ff7197b4c1868f25727e45@52.78.149.82:30303"  # noqa: E501
)
GOERLI_BOOTNODES = (
    "enode://011f758e6552d105183b1761c5e2dea0111bc20fd5f6422bc7f91e0fabbec9a6595caf6239b37feb773dddd3f87240d99d859431891e4a642cf2a0a9e6cbb98a@51.141.78.53:30303",  # noqa: E501
    "enode://176b9417f511d05b6b2cf3e34b756cf0a7096b3094572a8f6ef4cdcb9d1f9d00683bf0f83347eebdf3b81c3521c2332086d9592802230bf528eaf606a1d9677b@13.93.54.137:30303",  # noqa: E501
    "enode://46add44b9f13965f7b9875ac6b85f016f341012d84f975377573800a863526f4da19ae2c620ec73d11591fa9510e992ecc03ad0751f53cc02f7c7ed6d55c7291@94.237.54.114:30313",  # noqa: E501
    "enode://b5948a2d3e9d486c4d75bf32713221c2bd6cf86463302339299bd227dc2e276cd5a1c7ca4f43a0e9122fe9af884efed563bd2a1fd28661f3b5f5ad7bf1de5949@18.218.250.66:30303",  # noqa: E501
)
ROPSTEN_BOOTNODES = (
    'enode://15ac307a470b411745a6f10544ed54c0a14ad640b21f04f523e736e732bf709d4e28c2f06526ecabc03eed226b6d9bee8e433883cd20ab6cbd114bab77a8775d@52.176.7.10:30303',     # noqa: E501
    'enode://865a63255b3bb68023b6bffd5095118fcc13e79dcf014fe4e47e065c350c7cc72af2e53eff895f11ba1bbb6a2b33271c1116ee870f266618eadfc2e78aa7349c@52.176.100.77:30303',   # noqa: E501
    'enode://6332792c4a00e3e4ee0926ed89e0d27ef985424d97b6a45bf0f23e51f0dcb5e66b875777506458aea7af6f9e4ffb69f43f3778ee73c81ed9d34c51c4b16b0b0f@52.232.243.152:30303',  # noqa: E501
    'enode://94c15d1b9e2fe7ce56e458b9a3b672ef11894ddedd0c6f247e0f1d3487f52b66208fb4aeb8179fce6e3a749ea93ed147c37976d67af557508d199d9594c35f09@192.81.208.223:30303',  # noqa: E501
)
DISCOVERY_V5_BOOTNODES = (
    'enode://06051a5573c81934c9554ef2898eb13b33a34b94cf36b202b69fde139ca17a85051979867720d4bdae4323d4943ddf9aeeb6643633aa656e0be843659795007a@35.177.226.168:30303',  # noqa: E501
    'enode://0cc5f5ffb5d9098c8b8c62325f3797f56509bff942704687b6530992ac706e2cb946b90a34f1f19548cd3c7baccbcaea354531e5983c7d1bc0dee16ce4b6440b@40.118.3.223:30304',    # noqa: E501
    'enode://1c7a64d76c0334b0418c004af2f67c50e36a3be60b5e4790bdac0439d21603469a85fad36f2473c9a80eb043ae60936df905fa28f1ff614c3e5dc34f15dcd2dc@40.118.3.223:30306',    # noqa: E501
    'enode://85c85d7143ae8bb96924f2b54f1b3e70d8c4d367af305325d30a61385a432f247d2c75c45c6b4a60335060d072d7f5b35dd1d4c45f76941f62a4f83b6e75daaf@40.118.3.223:30307',    # noqa: E501
)

# Maximum peers number, we'll try to keep open connections up to this number of peers
DEFAULT_MAX_PEERS = 25

# Maximum allowed depth for chain reorgs.
MAX_REORG_DEPTH = 24

# Random sampling rate (i.e. every K-th) for header seal checks during light/fast sync. Apparently
# 100 was the optimal value determined by geth devs
# (https://github.com/ethereum/go-ethereum/pull/1889#issue-47241762), but in order to err on the
# side of caution, we use a higher value.
SEAL_CHECK_RANDOM_SAMPLE_RATE = 48


# The amount of time that the BasePeerPool will wait for a peer to boot before
# aborting the connection attempt.
DEFAULT_PEER_BOOT_TIMEOUT = 20

# How long before we timeout when waiting for a peer to be ready.
PEER_READY_TIMEOUT = 1

# Name of the endpoint that the discovery uses to connect to the eventbus
DISCOVERY_EVENTBUS_ENDPOINT = 'discovery'
# Interval at which peer pool requests new connection candidates
PEER_CONNECT_INTERVAL = 2

# Maximum number of sequential connection attempts that can be made before
# hitting the rate limit
MAX_SEQUENTIAL_PEER_CONNECT = 5

# Timeout used when fetching peer candidates from discovery
REQUEST_PEER_CANDIDATE_TIMEOUT = 1

# The maximum number of concurrent attempts to establis new peer connections
MAX_CONCURRENT_CONNECTION_ATTEMPTS = 10

# Amount of time a peer will be blacklisted when they are disconnected as
# `DisconnectReason.BAD_PROTOCOL`
BLACKLIST_SECONDS_BAD_PROTOCOL = 60 * 10  # 10 minutes

# Amount of time a peer will be blacklisted when they timeout too frequently
BLACKLIST_SECONDS_TOO_MANY_TIMEOUTS = 60 * 5  # 5 minutes

# Both the amount of time that we consider to be a peer disconnecting from us
# too quickly as well as the amount of time they will be blacklisted for doing
# so.
BLACKLIST_SECONDS_QUICK_DISCONNECT = 60

# Some logs are noisy, but we're very interested in them when the peer pool is small.
# Like we're eager to know about newly added peers. This constant defines the
# minimum pool size that we start quieting the logs.
QUIET_PEER_POOL_SIZE = 5

#
# Kademlia Constants
#

# number of bits per hop
KADEMLIA_BITS_PER_HOP = 8

# bucket size for kademlia routing table
KADEMLIA_BUCKET_SIZE = 16

# round trip message timout
KADEMLIA_REQUEST_TIMEOUT = 7.2

# Amount of time to consider a bucket idle
KADEMLIA_IDLE_BUCKET_REFRESH_INTERVAL = 3600

# Number of parallele `find_node` lookups that can be in progress
KADEMLIA_FIND_CONCURRENCY = 3

# Size of public keys in bits
KADEMLIA_PUBLIC_KEY_SIZE = 512

# Size of a node id in bits
KADEMLIA_ID_SIZE = 256

# Maximum node `id` for a kademlia node
KADEMLIA_MAX_NODE_ID = (2 ** KADEMLIA_ID_SIZE) - 1

KADEMLIA_BOND_EXPIRATION = 24 * 60

# Reserved command length for the base `p2p` protocol
# - https://github.com/ethereum/devp2p/blob/master/rlpx.md#message-id-based-multiplexing
P2P_PROTOCOL_COMMAND_LENGTH = 16

# RLPx header data
RLPX_HEADER_DATA = b'\xc2\x80\x80'  # rlp.encode([0, 0])

# Max size of discovery packets.
DISCOVERY_MAX_PACKET_SIZE = 1280

# Buffer size used for incoming discovery UDP datagrams (must be larger than
# DISCOVERY_MAX_PACKET_SIZE)
DISCOVERY_DATAGRAM_BUFFER_SIZE = DISCOVERY_MAX_PACKET_SIZE * 2

NEIGHBOURS_RESPONSE_ITEMS = 16
NUM_ROUTING_TABLE_BUCKETS = 256  # number of buckets in the routing table

# Stringified objects longer than this will be trimmed before emitting to log.
#   Some commands were >200k characters as a string, and choked up the
#   AsyncProcessRunner.stderr which overreaches the buffer limit in the
#   internal StreamReader.readuntil() call. The 10k limit is informed by:
#   - should be much smaller than the ~200k buffer limit (because some logs
#       might print multiple commands)
#   - should be big enough not to clip typical logs (On a test DEBUG2 run
#       on mainnet, the largest logs were <2k characters)
LONGEST_ALLOWED_LOG_STRING = 10000

# Decoding long messages can take a while. We don't want to hold the event
#   loop for too long. So we push the decoding into a thread pool if the
#   encoded message is more than this many bytes:
MAX_IN_LOOP_DECODE_SIZE = 2000
