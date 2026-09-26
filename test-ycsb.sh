#!/bin/bash

# Check input arguments
if [ $# -lt 2 ]; then
    echo "Usage: $0 <ycsb_option> <client_count>"
    echo "ycsb_option: 0=A, 1=B, 2=C, 3=D"
    exit 1
fi

YCSB_OPTION=$1
CLIENT_COUNT=$2

# Validate the YCSB option argument
if [ "$YCSB_OPTION" -lt 0 ] || [ "$YCSB_OPTION" -gt 3 ]; then
    echo "Error: ycsb_optionmust be between 0 and 3"
    exit 1
fi

# YCSB option name mapping
case $YCSB_OPTION in
    0) YCSB_NAME="a" ;;
    1) YCSB_NAME="b" ;;
    2) YCSB_NAME="c" ;;
    3) YCSB_NAME="d" ;;
esac

echo "Starting YCSB-$YCSB_NAMEtest, client count: $CLIENT_COUNT"

# # Initialize environment
# echo "Initializing CXL..."
./scripts/run.sh init 8

# Configure port-mode variables
SEQUENCER_HOST="127.0.0.1"
SERVER0_HOST="127.0.0.1"
SERVER1_HOST="127.0.0.1"
CLIENT0_HOST="127.0.0.1"
CLIENT1_HOST="127.0.0.1"
SEQUENCER_PORT=10022
SERVER0_PORT=10023
SERVER1_PORT=10024
CLIENT0_PORT=10029
CLIENT1_PORT=10029
SERVER0_ID=0
SERVER1_ID=1

# Clear logs directories on all machines first
echo "Clearing logs directories on all machines..."
ssh -p $SEQUENCER_PORT root@$SEQUENCER_HOST "rm -rf ~/rhodes/logs/*"
ssh -p $SERVER0_PORT root@$SERVER0_HOST "rm -rf ~/rhodes/logs/*"
ssh -p $SERVER1_PORT root@$SERVER1_HOST "rm -rf ~/rhodes/logs/*"
ssh -p $CLIENT0_PORT root@$CLIENT0_HOST "rm -rf ~/rhodes/logs/*"
ssh -p $CLIENT1_PORT root@$CLIENT1_HOST "rm -rf ~/rhodes/logs/*"
echo "All logs directories cleared"

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
    ssh -p $CLIENT0_PORT root@$CLIENT0_HOST "pkill -f client" 2>/dev/null || true
    ssh -p $CLIENT1_PORT root@$CLIENT1_HOST "pkill -f client" 2>/dev/null || true
    echo "Cleaned client processes"
    
    # Wait for processes to exit completely
    sleep 2
}

# Clean up processes at script start
cleanup_processes

# Starting sequencer
echo "Starting sequencer..."
ssh -p $SEQUENCER_PORT root@$SEQUENCER_HOST "cd ~/rhodes && ./sequencer" &
SEQUENCER_PID=$!

# Wait 1 second
sleep 10

# Starting server 0
echo "Starting server $SERVER0_ID..."
ssh -p $SERVER0_PORT root@$SERVER0_HOST "cd ~/rhodes && ./server $SERVER0_ID" &
SERVER0_PID=$!

# Starting server 1
echo "Starting server $SERVER1_ID..."
ssh -p $SERVER1_PORT root@$SERVER1_HOST "cd ~/rhodes && ./server $SERVER1_ID" &
SERVER1_PID=$!

# Wait 2 seconds
sleep 8

# Start clients - run all clients on client0
echo "Starting all clients..."

CLIENT0_COMMANDS=""


for ((i=0; i<CLIENT_COUNT; i++)); do
    SERVER_ID=$((i % 2))
    echo "Preparing to start client on client0 $i (assigned to server $SERVER_ID)..."
    CLIENT0_COMMANDS="$CLIENT0_COMMANDS cd ~/rhodes && ./client $i $SERVER_ID & "
done

# Start all clients on client0
if [ -n "$CLIENT0_COMMANDS" ]; then
    ssh -p $CLIENT0_PORT root@$CLIENT0_HOST "$CLIENT0_COMMANDS wait" &
    CLIENT0_SSH_PID=$!
fi

# Wait for all client processes on client0 to finish
if [ -n "$CLIENT0_COMMANDS" ]; then
    wait $CLIENT0_SSH_PID
    echo "All client processes on client0 completed"
fi

# Create result directory
RESULT_DIR="./result/ycsb-${YCSB_NAME}/client_${CLIENT_COUNT}"
mkdir -p $RESULT_DIR

# Copy CSV files for all clients
# echo "Copying CSV files to $RESULT_DIR..."
# for ((i=0; i<CLIENT_COUNT; i++)); do
#     scp -P $CLIENT0_PORT root@$CLIENT0_HOST:~/rhodes/$i.csv $RESULT_DIR/$i.csv
#     # echo "Copied CSV file for client $i to $RESULT_DIR/$i.csv"
# done

scp -P $CLIENT0_PORT root@$CLIENT0_HOST:~/rhodes/*.csv $RESULT_DIR/

echo "All CSV files copied to $RESULT_DIR"

# Cleaning remote CSV files
ssh -p $CLIENT0_PORT root@$CLIENT0_HOST "rm -f ~/rhodes/*.csv"

echo "Remote CSV cleanup complete"

# Collect logs from all machines into the local logs directory
LOCAL_LOGS_DIR="./logs"
mkdir -p $LOCAL_LOGS_DIR
echo "Copying log files from all machines to local $LOCAL_LOGS_DIR ..."

scp -P $SEQUENCER_PORT -r root@$SEQUENCER_HOST:~/rhodes/logs/* $LOCAL_LOGS_DIR/sequencer_log_ 2>/dev/null || true
scp -P $SERVER0_PORT -r root@$SERVER0_HOST:~/rhodes/logs/* $LOCAL_LOGS_DIR/server0_log_ 2>/dev/null || true
scp -P $SERVER1_PORT -r root@$SERVER1_HOST:~/rhodes/logs/* $LOCAL_LOGS_DIR/server1_log_ 2>/dev/null || true
scp -P $CLIENT0_PORT -r root@$CLIENT0_HOST:~/rhodes/logs/* $LOCAL_LOGS_DIR/client0_log_ 2>/dev/null || true
scp -P $CLIENT1_PORT -r root@$CLIENT1_HOST:~/rhodes/logs/* $LOCAL_LOGS_DIR/client1_log_ 2>/dev/null || true

echo "All log files copied to $LOCAL_LOGS_DIR"

# Clean up processes at script end
cleanup_processes

echo "Test complete; all processes cleaned up"
