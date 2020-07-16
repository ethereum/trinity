from eth.constants import MAX_UNCLE_DEPTH

from trinity.sync.common.constants import PREDICTED_BLOCK_TIME

# Peers are typically expected to have predicted nodes available,
#   so it's reasonable to ask for all-predictive nodes from a peer.
# Urgent node requests usually come in pretty fast, so
#   even at a small value (like 1ms), this timeout is rarely triggered.
DELAY_BEFORE_NON_URGENT_REQUEST = 0.05

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

# If a peer does something not ideal, give it a little time to breath,
# and maybe to try out another peeer. Then reinsert it relatively soon.
# Measured in seconds.
NON_IDEAL_RESPONSE_PENALTY = 0.5

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
# The value is configurable by client, but tends to be around 75 blocks.
ESTIMATED_BEAMABLE_BLOCKS = 75

# It's also useful to estimate the amount of time covered by those beamable blocks.
ESTIMATED_BEAMABLE_SECONDS = ESTIMATED_BEAMABLE_BLOCKS * PREDICTED_BLOCK_TIME

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
