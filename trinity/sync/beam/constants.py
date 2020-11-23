from eth.constants import MAX_UNCLE_DEPTH

from trinity.sync.common.constants import PREDICTED_BLOCK_TIME

# If we are waiting for predictive nodes requests to come in and
#   they don't for this long, then reduce the minimum predictive
#   peer response.
# Also, if we are waiting for a peer to ask for predictive nodes
#   for this long, then maybe increase the number of predictive peers.
# Picked this value from thin air.
TOO_LONG_PREDICTIVE_PEER_DELAY = 2.0

# How much large should our buffer be? This is a multiplier on how many
# nodes we can request at once from a single peer.
REQUEST_BUFFER_MULTIPLIER = 16

# How many different processes are running previews? They will split the
# block imports equally. A higher number means a slower startup, but more
# previews are possible at a time (given that you have enough CPU cores).
# The sensitivity of this number is relatively unexplored.
NUM_PREVIEW_SHARDS = 4

# How many speculative executions should we run concurrently? This is
#   a global number, not per process or thread. It is necessary to
#   constrain the I/O, which can become the global bottleneck.
MAX_CONCURRENT_SPECULATIVE_EXECUTIONS = 40
MAX_SPECULATIVE_EXECUTIONS_PER_PROCESS = MAX_CONCURRENT_SPECULATIVE_EXECUTIONS // NUM_PREVIEW_SHARDS

# How many seconds to wait in between each progress log, in the middle of a block
#   Intuition: Report about 5 times per block. If progressing in at least real time,
#   then we should see the % jump by ~20% in each report.
MIN_GAS_LOG_WAIT = PREDICTED_BLOCK_TIME / 5

# If a peer does something not ideal, give it a little time to breath,
# and maybe to try out another peeer. Then reinsert it relatively soon.
# Measured in seconds.
NON_IDEAL_RESPONSE_PENALTY = 2.0

# Sometimes, Beam Sync will ask for the "urgent" trie node from more than one
#   peer, to reduce the latency. We call this "Spread Beam". When a request for
#   a single urgent node takes longer than this many seconds to return, we
#   increase the number of peers that we ask for urgent nodes, the spread beam factor.
# Where did this number come from? Rough estimates: say that we need to import
#   a block within a pivot, which happens every ~120 blocks. At 13 seconds per block,
#   that's ~1500 seconds. We download ~4000 new trie nodes, with high variance on the
#   block. 1500 / 4000 ~= 0.4
MAX_ACCEPTABLE_WAIT_FOR_URGENT_NODE = 0.4

# How long before the block importer gives up on waiting for a missing trie
#   node, and reissues the event to request it again.
BLOCK_IMPORT_MISSING_STATE_TIMEOUT = 600

# If Beam Sync wants to use a queen, but is stuck waiting for it to show up,
#   then log a warning if it's been too long. If it's been more than this
#   many seconds, then log the warning:
WARN_AFTER_QUEEN_STARVED = 0.1

# How many seconds should we leave the backfill peer idle, in between
# backfill requests? This is called "tests" because we are importantly
# checking how fast a peer is.
GAP_BETWEEN_TESTS = 0.25
# One reason to leave this as non-zero is: if we are regularly switching
# the "queen peer" then we want to improve the chances that the new queen
# (formerly backfill) is idle and ready to serve urgent nodes.
# Another reason to leave this as non-zero: we don't want to overload the
# database with reads/writes, but there are probably better ways to acheive
# that goal.
# One reason to make it relatively short, is that we want to find out quickly
# when a new peer has excellent service stats. It might take several requests
# to establish it (partially because we measure using an exponential average).

# About how long after a block can we request trie data from peers?
# The value is configurable by client, but tends to be around 120 blocks.
ESTIMATED_BEAMABLE_BLOCKS = 120

# It's also useful to estimate the amount of time covered by those beamable blocks.
ESTIMATED_BEAMABLE_SECONDS = ESTIMATED_BEAMABLE_BLOCKS * PREDICTED_BLOCK_TIME

# Maximum number of blocks we can lag behind the current chain head before we pivot.
# This is a relatively low value because on mainnet, as soon as we are lagging behind 20-30
# blocks, we're very unlikely to catch up given our block import times are high.
MAX_BEAM_SYNC_LAG = 30

# To make up for clients that are configured with unusually low block times,
# and other surprises, we pivot earlier than we think we need to.
# For example, if the BEAM_PIVOT_BUFFER_FRACTION is ~1/4, then pivot about 25%
#   earlier than estimated, to reduce the risk of getting temporarily stuck.
BEAM_PIVOT_BUFFER_FRACTION = 1 / 2

# We need MAX_UNCLE_DEPTH + 1 headers to check during uncle validation
# We need to request one more header, to set the starting tip
FULL_BLOCKS_NEEDED_TO_START_BEAM = MAX_UNCLE_DEPTH + 2

# If backfill is paused, resume when the lag has reduced to this number of blocks.
RESUME_BACKFILL_AT_LAG = 0

# Pause backfill if sync starts lagging behind this number of blocks
PAUSE_BACKFILL_AT_LAG = 5

# If there is still unknown state to request, but we don't have any available hashes
#   to ask for a peer (all known hashes are already being actively requested), then
#   pause this long before asking any other peer for more state hashes.
PAUSE_SECONDS_IF_STATE_BACKFILL_STARVED = 2

# Rotate the state backfill key to a new state root once every epoch. The key is
#   used to have beam syncing nodes generally be able to help each other backfill.
#   Otherwise, they might all haphazardly try to fill in different parts of the trie
#   at the same time, and not have the nodes needed to supply each other. So all
#   nodes try to get nodes near the key defined by the state root on every block with
#   block_number % EPOCH_BLOCK_LENGTH == 0.
EPOCH_BLOCK_LENGTH = 32

# The time that the block backfill should idle when there are concurrently no blocks to fill.
# Once gaps are closed they can only re-occur when beam sync pivots so it's ok to idle a fair while.
BLOCK_BACKFILL_IDLE_TIME = PREDICTED_BLOCK_TIME * 500

# Preview blocks might be paused, waiting on data that comes in through another avenue, like
#   urgent data requests, or backfill. Use the following period to check for new data.
CHECK_PREVIEW_STATE_TIMEOUT = 20.0
