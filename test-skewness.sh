#!/bin/bash
set -euo pipefail

# This script tests the counter workload with fixed total client counts and skewness values.
CLIENT_COUNTS=(96 140)
ZIPF_PARAMS=(0 0.7 0.9 0.99)
EXPERIMENT_RUNS=3
SUMMARY_FILE="./result/skewness/summary.csv"

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

initialize_summary_file() {
    mkdir -p "$(dirname "$SUMMARY_FILE")"
    echo "zipf_theta,client_count,run,total_requests,total_time_seconds,throughput,p50_latency,p99_latency" > "$SUMMARY_FILE"
}

analyze_run_result() {
    local result_dir=$1
    local zipf_param=$2
    local client_count=$3
    local run_idx=$4

    python3 - "$result_dir" "$zipf_param" "$client_count" "$run_idx" "$SUMMARY_FILE" <<'PY'
import csv
import math
import sys
from pathlib import Path

result_dir = Path(sys.argv[1])
zipf_theta = sys.argv[2]
client_count = int(sys.argv[3])
run_idx = int(sys.argv[4])
summary_file = Path(sys.argv[5])

csv_files = sorted(result_dir.glob("*.csv"), key=lambda p: p.name)
if len(csv_files) != client_count:
    sys.exit(f"Expected {client_count} client CSV files in {result_dir}, got {len(csv_files)}")

latencies = []
start_times = []
end_times = []

for csv_file in csv_files:
    valid_rows = 0
    with csv_file.open(newline="") as fh:
        for row in csv.reader(fh):
            if len(row) < 3:
                continue
            try:
                start_time = float(row[0])
                end_time = float(row[1])
                latency = float(row[2])
            except ValueError:
                continue
            start_times.append(start_time)
            end_times.append(end_time)
            latencies.append(latency)
            valid_rows += 1
    if valid_rows != 19980:
        sys.exit(f"{csv_file}: expected 19980 measured requests, got {valid_rows}")

if not latencies:
    print(f"No valid latency records found in {result_dir}")
    sys.exit(1)

latencies.sort()

def percentile(values, percent):
    if len(values) == 1:
        return values[0]
    pos = (len(values) - 1) * percent / 100.0
    lower = math.floor(pos)
    upper = math.ceil(pos)
    if lower == upper:
        return values[int(pos)]
    return values[lower] * (upper - pos) + values[upper] * (pos - lower)

total_requests = len(latencies)
total_time_seconds = (max(end_times) - min(start_times)) / 1e6
throughput = total_requests / total_time_seconds if total_time_seconds > 0 else 0.0
p50_latency = percentile(latencies, 50)
p99_latency = percentile(latencies, 99)

summary_file.parent.mkdir(parents=True, exist_ok=True)
write_header = not summary_file.exists() or summary_file.stat().st_size == 0
with summary_file.open("a", newline="") as fh:
    writer = csv.DictWriter(
        fh,
        fieldnames=[
            "zipf_theta",
            "client_count",
            "run",
            "total_requests",
            "total_time_seconds",
            "throughput",
            "p50_latency",
            "p99_latency",
        ],
    )
    if write_header:
        writer.writeheader()
    writer.writerow({
        "zipf_theta": zipf_theta,
        "client_count": client_count,
        "run": run_idx,
        "total_requests": total_requests,
        "total_time_seconds": f"{total_time_seconds:.6f}",
        "throughput": f"{throughput:.6f}",
        "p50_latency": f"{p50_latency:.6f}",
        "p99_latency": f"{p99_latency:.6f}",
    })

print("\n=== Skewness run performance ===")
print(f"Zipf theta: {zipf_theta}")
print(f"Client count: {client_count}")
print(f"Run: {run_idx}")
print(f"CSV files: {len(csv_files)}")
print(f"Total requests: {total_requests}")
print(f"Total time: {total_time_seconds:.2f} seconds")
print(f"Throughput: {throughput:.2f} requests/sec")
print(f"P50 latency: {p50_latency:.2f} us")
print(f"P99 latency: {p99_latency:.2f} us")
print("")
PY
}

print_average_results() {
    python3 - "$SUMMARY_FILE" "$EXPERIMENT_RUNS" <<'PY'
import csv
import sys
from collections import defaultdict
from pathlib import Path

summary_file = Path(sys.argv[1])
expected_runs = int(sys.argv[2])

if not summary_file.exists():
    print(f"No summary file found: {summary_file}")
    sys.exit(0)

with summary_file.open(newline="") as fh:
    rows = list(csv.DictReader(fh))

if not rows:
    print("No successful skewness run results were recorded.")
    sys.exit(0)

groups = defaultdict(list)
for row in rows:
    groups[(float(row["zipf_theta"]), int(row["client_count"]))].append(row)

print("\n=== Average performance by skewness setting ===")
print(f"{'Zipf theta':>10} {'Clients':>8} {'Runs':>6} {'Avg throughput(req/s)':>24} {'Avg P50(us)':>14} {'Avg P99(us)':>14}")

for zipf_theta, client_count in sorted(groups, key=lambda item: (item[1], item[0])):
    setting_rows = groups[(zipf_theta, client_count)]
    run_count = len({row["run"] for row in setting_rows})

    def avg(field):
        return sum(float(row[field]) for row in setting_rows) / len(setting_rows)

    print(
        f"{zipf_theta:>10:g} "
        f"{client_count:>8} "
        f"{run_count:>6}/{expected_runs:<2} "
        f"{avg('throughput'):>24.2f} "
        f"{avg('p50_latency'):>14.2f} "
        f"{avg('p99_latency'):>14.2f}"
    )

print(f"\nSummary CSV: {summary_file}")
PY
}

# Function to update the config file and sync it to VMs
update_config() {
    local zipf_param=$1
    local client_count=$2
    local config_client_num=$(((client_count + 1) / 2))

    echo "Updating config file: workload=3, request_worker_num=2, zipf_theta=$zipf_param, client_num=$config_client_num"

    if command -v jq &> /dev/null; then
        jq --arg zt "$zipf_param" \
           --arg cn "$config_client_num" \
           '.workload = 3 |
            .request_worker_num = 2 |
            .zipf_theta = ($zt | tonumber) |
            .client_num = ($cn | tonumber)' \
           ./src/common/config.json > ./src/common/config.json.tmp && mv ./src/common/config.json.tmp ./src/common/config.json
    else
        sed -i "s/\"workload\": [0-9]*/\"workload\": 3/" ./src/common/config.json
        sed -i "s/\"request_worker_num\": [0-9]*/\"request_worker_num\": 2/" ./src/common/config.json
        sed -i "s/\"zipf_theta\": [0-9.]*[0-9]/\"zipf_theta\": $zipf_param/" ./src/common/config.json
        sed -i "s/\"client_num\": [0-9]*/\"client_num\": $config_client_num/" ./src/common/config.json
    fi

    echo "Config file updated"
    cat ./src/common/config.json

    echo "Syncing the config file to all machines..."
    ./scripts/run.sh sync 8
    sleep 2
    echo "Config file sync complete"
}

# Function to run one skewness test
run_single_test() {
    local zipf_param=$1
    local client_count=$2
    local run_idx=$3

    echo "=========================================="
    echo "Starting counter test: zipf_theta=$zipf_param, client_count=$client_count, run=$run_idx/$EXPERIMENT_RUNS"
    echo "=========================================="

    update_config "$zipf_param" "$client_count"

    cleanup_processes

    echo "Initializing CXL..."
    ./scripts/run.sh init 8

    cleanup_processes

    # Starting sequencer
    echo "Starting sequencer..."
    ssh -p $SEQUENCER_PORT root@$SEQUENCER_HOST "cd ~/rhodes && ./sequencer" &
    SEQUENCER_PID=$!

    # Wait for sequencer initialization
    sleep 5

    # Starting server 0
    echo "Starting server $SERVER0_ID..."
    ssh -p $SERVER0_PORT root@$SERVER0_HOST "cd ~/rhodes && ./server $SERVER0_ID" &
    SERVER0_PID=$!

    # Starting server 1
    echo "Starting server $SERVER1_ID..."
    ssh -p $SERVER1_PORT root@$SERVER1_HOST "cd ~/rhodes && ./server $SERVER1_ID" &
    SERVER1_PID=$!

    # Wait for server initialization
    sleep 5

    # Start clients - use one SSH connection to start all clients
    echo "Cleaning remote CSV files before starting clients..."
    ssh -p $CLIENT_PORT root@$CLIENT_HOST "rm -f ~/rhodes/*.csv" 2>/dev/null || true

    echo "Starting all clients..."
    CLIENT_COMMANDS='pids=""; '
    for ((i=0; i<client_count; i++)); do
        # Modulo assignment: even clients go to server 0, odd clients go to server 1
        SERVER_ID=$((i % 2))

        echo "Preparing to start client $i (assigned to server $SERVER_ID)..."
        CLIENT_COMMANDS+="(cd ~/rhodes && ./client $i $SERVER_ID) & pids=\"\$pids \$!\"; "
    done

    CLIENT_COMMANDS+='status=0; for pid in $pids; do wait "$pid" || status=1; done; exit "$status"'
    timeout 120 ssh -p $CLIENT_PORT root@$CLIENT_HOST "$CLIENT_COMMANDS"
    echo "All client processes completed"

    # Create result directory
    RESULT_DIR="./result/skewness/${zipf_param}/client_${client_count}/run_${run_idx}"
    rm -rf "$RESULT_DIR"
    mkdir -p "$RESULT_DIR"

    # Copy CSV files for all clients
    echo "Copying CSV files to $RESULT_DIR..."
    for ((i=0; i<client_count; i++)); do
        scp -P $CLIENT_PORT root@$CLIENT_HOST:~/rhodes/$i.csv "$RESULT_DIR/$i.csv"
        if [ -f "$RESULT_DIR/$i.csv" ]; then
            echo "Copied CSV file for client $i to $RESULT_DIR/$i.csv"
        else
            echo "Warning: client $i CSV file copy failed"
        fi
    done

    echo "All CSV files copied to $RESULT_DIR"

    # Cleaning remote CSV files
    echo "Cleaning remote CSV files..."
    for ((i=0; i<client_count; i++)); do
        ssh -p $CLIENT_PORT root@$CLIENT_HOST "rm -f ~/rhodes/$i.csv" 2>/dev/null || true
    done

    # Clean up processes
    cleanup_processes

    analyze_run_result "$RESULT_DIR" "$zipf_param" "$client_count" "$run_idx"

    echo "Test complete: zipf_theta=$zipf_param, client_count=$client_count, run=$run_idx"
    echo "Results saved in: $RESULT_DIR"
    echo ""
}

# Main test loop
trap cleanup_processes EXIT
echo "Starting counter skewness test"
echo "Test parameter combinations:"
echo "  Zipf theta: ${ZIPF_PARAMS[@]}"
echo "  Client count: ${CLIENT_COUNTS[@]}"
echo "  Experiment runs per setting: $EXPERIMENT_RUNS"
echo ""

initialize_summary_file

TOTAL_TESTS=$((${#ZIPF_PARAMS[@]} * ${#CLIENT_COUNTS[@]} * EXPERIMENT_RUNS))
CURRENT_TEST=0

for ((run_idx=1; run_idx<=EXPERIMENT_RUNS; run_idx++)); do
    echo "=========================================="
    echo "Starting experiment run $run_idx/$EXPERIMENT_RUNS"
    echo "=========================================="

    for client_count in "${CLIENT_COUNTS[@]}"; do
        for zipf_param in "${ZIPF_PARAMS[@]}"; do
            CURRENT_TEST=$((CURRENT_TEST + 1))
            echo "Progress: $CURRENT_TEST/$TOTAL_TESTS"
            run_single_test "$zipf_param" "$client_count" "$run_idx"

            # Pause briefly between tests
            if [ $CURRENT_TEST -lt $TOTAL_TESTS ]; then
                echo "Waiting 5 seconds before the next test..."
                sleep 5
            fi
        done
    done
done

print_average_results

echo "=========================================="
echo "All tests complete!"
echo "=========================================="
echo "Test results saved in: ./result/skewness/"
echo "Directory structure:"
echo "  ./result/skewness/{zipf_theta}/client_{96,140}/run_{1,2,3}/"
echo "Contains:"
echo "  - *.csv: client performance data"
echo "  - summary.csv: per-run metrics and final average input"
