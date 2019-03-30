#!/bin/bash
# TODO
#   - spin up alice and bob
#   - remove the root dir

DIR_ROOT="/tmp/ttt"
DIR_ALICE="$DIR_ROOT/alice"
DIR_BOB="$DIR_ROOT/bob"
EXE_GEN_GENESIS="$(dirname $0)/gen_genesis_json.py"
PORT_ALICE=30304
PORT_BOB=30305
INDEX_ALICE=0
INDEX_BOB=1
TIME_BOB_AFTER_ALICE=30


CMD_ALICE="trinity-beacon --mock-blocks=true --validator_index=$INDEX_ALICE --port=$PORT_ALICE --trinity-root-dir=$DIR_ALICE --beacon-nodekey=6b94ffa2d9b8ee85afb9d7153c463ea22789d3bbc5d961cc4f63a41676883c19 -l debug"
CMD_BOB="trinity-beacon --validator_index=$INDEX_BOB --port=$PORT_BOB --trinity-root-dir=$DIR_BOB --port=5566 --beacon-nodekey=f5ad1c57b5a489fc8f21ad0e5a19c1f1a60b8ab357a2100ff7e75f3fa8a4fd2e --bootstrap_nodes=enode://c289557985d885a3f13830a475d649df434099066fbdc840aafac23144f6ecb70d7cc16c186467f273ad7b29707aa15e6a50ec3fde35ae2e69b07b3ddc7a36c7@0.0.0.0:30303  -l debug"

# generate the genesis.json

$EXE_GEN_GENESIS

rm -rf $DIR_ALICE $DIR_BOB >/dev/null
mkdir -p $DIR_ALICE $DIR_BOB >/dev/null

echo "Spinning up ALICE"
$CMD_ALICE &
PID_ALICE=$!
PIDS_TO_KILL=$PID_ALICE

trap "echo \"Killing nodes...\"; kill -9 $PIDS_TO_KILL; echo '\nDone\n'; exit;" SIGINT

echo "Sleeping $TIME_BOB_AFTER_ALICE seconds to wait until ALICE is initialized"
sleep $TIME_BOB_AFTER_ALICE

echo "Spinning up BOB"
$CMD_BOB &
PID_BOB=$!
PIDS_TO_KILL="$PID_ALICE $PID_BOB"
trap "echo \"Killing nodes...\"; kill -9 $PIDS_TO_KILL; echo '\nDone\n'; exit;" SIGINT

# trap "echo \"Killing nodes...\"; ps aux| egrep 'python.*trinity-beacon' | awk '{print $2;}' | xargs kill; echo '\nDone\n'; exit;" SIGINT
sleep 1000000000
