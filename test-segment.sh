#!/bin/bash

# Check input arguments
if [ $# -lt 1 ]; then
    echo "Usage: $0 <client_count> [segment_size1] [segment_size2] ..."
    echo "Workload: YCSB-A (workload=1, ycsb_option=0)"
    echo "If segment_size is not specified, the default values are used: 320 576 1088 2112 4160"
    exit 1
fi

CLIENT_COUNT=$1
CONFIG_CLIENT_NUM=$(((CLIENT_COUNT + 1) / 2))
CONFIG_REQUEST_WORKER_NUM=1
if [ "$CLIENT_COUNT" -gt 2 ]; then
    CONFIG_REQUEST_WORKER_NUM=2
fi

# Get the list of segment_size values to test
if [ $# -ge 2 ]; then
    # Get the segment_size list from command-line arguments
    SEGMENT_SIZES=("${@:2}")
else
    # Default segment_size list
    SEGMENT_SIZES=(320 576 1088 2112 4160)
fi

echo "Testing the following segment_size values: ${SEGMENT_SIZES[@]}"
echo "workload: YCSB-A (workload=1, ycsb_option=0)"
echo "client_count: $CLIENT_COUNT"
echo "config client_num: $CONFIG_CLIENT_NUM"
echo "config request_worker_num: $CONFIG_REQUEST_WORKER_NUM"

# Configure port-mode variables
SEQUENCER_HOST="127.0.0.1"
SERVER0_HOST="127.0.0.1"
SERVER1_HOST="127.0.0.1"
CLIENT_HOST="127.0.0.1"
SEQUENCER_PORT=10022
SERVER0_PORT=10023
SERVER1_PORT=10024
CLIENT_PORT=10025
SERVER0_ID=0
SERVER1_ID=1

# Modify the segment_size field in config.json
update_segment_size() {
    local segment_size=$1
    local config_file="src/common/config.json"

    # Use jq to modify the JSON file(if available)
    if command -v jq &> /dev/null; then
        jq --arg segment_size "$segment_size" \
           '.segment_size = ($segment_size | tonumber)' \
           $config_file > $config_file.tmp
        mv $config_file.tmp $config_file
    else
        # Use sed when jq is unavailable.
        sed -i "s/\"segment_size\": [0-9]*/\"segment_size\": $segment_size/" $config_file
    fi

    echo "Updated config.json: segment_size=$segment_size"
}

# Configure YCSB-A explicitly, including when called after the counter skewness sweep.
update_segment_test_config() {
    local config_file="src/common/config.json"

    if command -v jq &> /dev/null; then
        jq --argjson client_num "$CONFIG_CLIENT_NUM" \
           --argjson request_worker_num "$CONFIG_REQUEST_WORKER_NUM" \
           '.workload = 1 |
            .segment_test = true |
            .ycsb_option = 0 |
            .request_worker_num = $request_worker_num |
            .client_num = $client_num' \
           $config_file > $config_file.tmp
        mv $config_file.tmp $config_file
    else
        if grep -q '"segment_test"' $config_file; then
            sed -i "s/\"segment_test\": \\(true\\|false\\)/\"segment_test\": true/" $config_file
        else
            sed -i '/"segment_size":/a\  "segment_test": true,' $config_file
        fi
        sed -i "s/\"workload\": [0-9]*/\"workload\": 1/" $config_file
        sed -i "s/\"ycsb_option\": [0-9]*/\"ycsb_option\": 0/" $config_file
        sed -i "s/\"request_worker_num\": [0-9]*/\"request_worker_num\": $CONFIG_REQUEST_WORKER_NUM/" $config_file
        sed -i "s/\"client_num\": [0-9]*/\"client_num\": $CONFIG_CLIENT_NUM/" $config_file
    fi

    echo "Updated config.json: workload=1 (YCSB-A), segment_test=true, ycsb_option=0, request_worker_num=$CONFIG_REQUEST_WORKER_NUM, client_num=$CONFIG_CLIENT_NUM"
}

# Restore segment-test-only config after the segment-size sweep.
restore_segment_test_config() {
    local config_file="src/common/config.json"

    if command -v jq &> /dev/null; then
        jq '.segment_test = false' \
           $config_file > $config_file.tmp
        mv $config_file.tmp $config_file
    else
        if grep -q '"segment_test"' $config_file; then
            sed -i "s/\"segment_test\": \\(true\\|false\\)/\"segment_test\": false/" $config_file
        else
            sed -i '/"segment_size":/a\  "segment_test": false,' $config_file
        fi
    fi

    echo "Updated config.json: segment_test=false"
}

# Cleanup function: kill all related processes
cleanup_processes() {
    echo "Cleaning all related processes..."

    # kill sequencer
    ssh -p $SEQUENCER_PORT root@$SEQUENCER_HOST "pkill -f sequencer" 2>/dev/null || true
    echo "Cleaned sequencer processes"

    # kill servers
    ssh -p $SERVER0_PORT root@$SERVER0_HOST "pkill -f server" 2>/dev/null || true
    ssh -p $SERVER1_PORT root@$SERVER1_HOST "pkill -f server" 2>/dev/null || true
    echo "Cleaned server processes"

    # kill clients
    ssh -p $CLIENT_PORT root@$CLIENT_HOST "pkill -f client" 2>/dev/null || true
    echo "Cleaned client processes"

    # Wait for processes to exit completely
    sleep 2
}

# Run one segment-size test.
run_single_test() {
    local segment_size=$1

    echo "=========================================="
    echo "Starting test segment_size=$segment_size"
    echo "=========================================="

    # Modify the segment_size field in config.json
    update_segment_size $segment_size

    # Upload config.json to the VMs
    echo "Upload config.json to the VMs..."
    ./scripts/run.sh sync 8
    echo "config.json upload complete"

    # Initialize environment
    echo "Initializing CXL..."
    ./scripts/run.sh init 8

    # Clear logs directories on all machines first
    echo "Clearing logs directories on all machines..."
    ssh -p $SEQUENCER_PORT root@$SEQUENCER_HOST "rm -rf ~/rhodes/logs/*"
    ssh -p $SERVER0_PORT root@$SERVER0_HOST "rm -rf ~/rhodes/logs/*"
    ssh -p $SERVER1_PORT root@$SERVER1_HOST "rm -rf ~/rhodes/logs/*"
    ssh -p $CLIENT_PORT root@$CLIENT_HOST "rm -rf ~/rhodes/logs/*"
    echo "All logs directories cleared"

    # Clean up processes
    cleanup_processes

    # Starting sequencer
    echo "Starting sequencer..."
    ssh -p $SEQUENCER_PORT root@$SEQUENCER_HOST "cd ~/rhodes && ./sequencer" &
    SEQUENCER_PID=$!

    # Wait 1 second
    sleep 5

    # Starting server 0
    echo "Starting server $SERVER0_ID..."
    ssh -p $SERVER0_PORT root@$SERVER0_HOST "cd ~/rhodes && ./server $SERVER0_ID" &
    SERVER0_PID=$!

    # Starting server 1
    echo "Starting server $SERVER1_ID..."
    ssh -p $SERVER1_PORT root@$SERVER1_HOST "cd ~/rhodes && ./server $SERVER1_ID" &
    SERVER1_PID=$!

    # Wait 2 seconds
    sleep 5

    # Start clients - use one SSH connection to start all clients
    echo "Starting all clients..."
    CLIENT_COMMANDS=""
    for ((i=0; i<CLIENT_COUNT; i++)); do
        # Modulo assignment: even clients go to server 0, odd clients go to server 1
        SERVER_ID=$((i % 2))

        echo "Preparing to start client $i (assigned to server $SERVER_ID)..."
        CLIENT_COMMANDS="$CLIENT_COMMANDS cd ~/rhodes && ./client $i $SERVER_ID & "
    done

    # Use one SSH connection to start all clients
    ssh -p $CLIENT_PORT root@$CLIENT_HOST "$CLIENT_COMMANDS wait"
    echo "All client processes completed"

    # Create result directory
    RESULT_DIR="./result/segment/segment_size_${segment_size}/client_${CLIENT_COUNT}"
    mkdir -p $RESULT_DIR

    # Copy CSV files for all clients
    echo "Copying CSV files to $RESULT_DIR..."
    scp -P $CLIENT_PORT root@$CLIENT_HOST:~/rhodes/*.csv $RESULT_DIR/ 2>/dev/null || true

    echo "All CSV files copied to $RESULT_DIR"

    # Cleaning remote CSV files
    echo "Cleaning remote CSV files..."
    ssh -p $CLIENT_PORT root@$CLIENT_HOST "rm -f ~/rhodes/*.csv"

    echo "Remote CSV cleanup complete"

    # # Collect logs from all machines into the local logs directory
    # LOCAL_LOGS_DIR="./logs/segment/segment_size_${segment_size}/client_${CLIENT_COUNT}"
    # mkdir -p $LOCAL_LOGS_DIR
    # echo "Copying log files from all machines to local $LOCAL_LOGS_DIR ..."

    # scp -P $SEQUENCER_PORT -r root@$SEQUENCER_HOST:~/rhodes/logs/* $LOCAL_LOGS_DIR/sequencer_log_ 2>/dev/null || true
    # scp -P $SERVER0_PORT -r root@$SERVER0_HOST:~/rhodes/logs/* $LOCAL_LOGS_DIR/server0_log_ 2>/dev/null || true
    # scp -P $SERVER1_PORT -r root@$SERVER1_HOST:~/rhodes/logs/* $LOCAL_LOGS_DIR/server1_log_ 2>/dev/null || true
    # scp -P $CLIENT_PORT -r root@$CLIENT_HOST:~/rhodes/logs/* $LOCAL_LOGS_DIR/client_log_ 2>/dev/null || true

    echo "All log files copied to $LOCAL_LOGS_DIR"

    # Clean up processes
    cleanup_processes

    echo "segment_size=$segment_size Test complete"
    echo ""
}

# Main loop: test every segment size.
update_segment_test_config

for segment_size in "${SEGMENT_SIZES[@]}"; do
    run_single_test $segment_size
done

echo "Resetting segment_size to default 2112 and segment_test=false..."
update_segment_size 2112
restore_segment_test_config
./scripts/run.sh sync 8
echo "segment_size reset and segment_test restore complete"

echo "=========================================="
echo "All segment-size tests complete"
echo "=========================================="
