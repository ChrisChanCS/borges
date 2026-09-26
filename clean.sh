# Configuration for VMs reached through forwarded SSH ports.
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

# Cleanup function: kill all related processes(forwarded SSH ports)
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
