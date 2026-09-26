#!/bin/bash

# Check input arguments
if [ $# -lt 1 ]; then
    echo "Usage: $0 <client_count>"
    exit 1
fi

CLIENT_COUNT=$1

# Initialize environment
echo "Initializing CXL..."
./scripts/run.sh init 8

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

# Clear logs directories on all machines first
echo "Clearing logs directories on all machines..."
ssh -p $SEQUENCER_PORT root@$SEQUENCER_HOST "rm -rf ~/rhodes/logs/*"
ssh -p $SERVER0_PORT root@$SERVER0_HOST "rm -rf ~/rhodes/logs/*"
ssh -p $SERVER1_PORT root@$SERVER1_HOST "rm -rf ~/rhodes/logs/*"
ssh -p $CLIENT_PORT root@$CLIENT_HOST "rm -rf ~/rhodes/logs/*"
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
    ssh -p $CLIENT_PORT root@$CLIENT_HOST "pkill -f client" 2>/dev/null || true
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
sleep 8

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
    # SERVER_ID=$((i % 2))
    SERVER_ID=$((i % 2))

    echo "Preparing to start client $i (assigned to server $SERVER_ID)..."
    CLIENT_COMMANDS="$CLIENT_COMMANDS cd ~/rhodes && ./client $i $SERVER_ID & "
done

# Use one SSH connection to start all clients
ssh -p $CLIENT_PORT root@$CLIENT_HOST "$CLIENT_COMMANDS wait"
echo "All client processes completed"

# Create result directory
RESULT_DIR="./result/retwis/client_${CLIENT_COUNT}"
mkdir -p $RESULT_DIR

# Copy CSV files for all clients
echo "Copying CSV files to $RESULT_DIR..."
# for ((i=0; i<CLIENT_COUNT; i++)); do
#     scp -P $CLIENT_PORT root@$CLIENT_HOST:~/rhodes/$i.csv $RESULT_DIR/$i.csv
#     echo "Copied CSV file for client $i to $RESULT_DIR/$i.csv"
# done

scp -P $CLIENT_PORT root@$CLIENT_HOST:~/rhodes/*.csv $RESULT_DIR/

echo "All CSV files copied to $RESULT_DIR"

# Cleaning remote CSV files
echo "Cleaning remote CSV files..."
# for ((i=0; i<CLIENT_COUNT; i++)); do
#     ssh -p $CLIENT_PORT root@$CLIENT_HOST "rm -f ~/rhodes/$i.csv"
#     echo "Cleaned CSV file for client $i"
# done

ssh -p $CLIENT_PORT root@$CLIENT_HOST "rm -f ~/rhodes/*.csv"

echo "Remote CSV cleanup complete"

# Collect logs from all machines into the local logs directory
LOCAL_LOGS_DIR="./logs"
mkdir -p $LOCAL_LOGS_DIR
echo "Copying log files from all machines to local $LOCAL_LOGS_DIR ..."

scp -P $SEQUENCER_PORT -r root@$SEQUENCER_HOST:~/rhodes/logs/* $LOCAL_LOGS_DIR/sequencer_log_ 2>/dev/null || true
scp -P $SERVER0_PORT -r root@$SERVER0_HOST:~/rhodes/logs/* $LOCAL_LOGS_DIR/server0_log_ 2>/dev/null || true
scp -P $SERVER1_PORT -r root@$SERVER1_HOST:~/rhodes/logs/* $LOCAL_LOGS_DIR/server1_log_ 2>/dev/null || true
scp -P $CLIENT_PORT -r root@$CLIENT_HOST:~/rhodes/logs/* $LOCAL_LOGS_DIR/client_log_ 2>/dev/null || true

echo "All log files copied to $LOCAL_LOGS_DIR"

# Clean up processes at script end
cleanup_processes

echo "Test complete; all processes cleaned up"
