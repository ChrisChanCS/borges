#!/bin/bash

# Exit on error
set -e

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Back up the original config.json
backup_config() {
    # Do not back up config.json
    log_info "Skipping config.json backup"
}

# Restore the original config.json
restore_config() {
    # Do not restore config.json
    log_info "Skipping config.json restore"
}

# Function to modify config.json
update_config() {
    local workload=$1
    local client_num=$2
    local request_worker_num=$3
    local ycsb_option=${4:-0}  # Default is 0

    # Use jq to modify the JSON file
    if command -v jq &> /dev/null; then
        jq --arg workload "$workload" \
           --arg client_num "$client_num" \
           --arg request_worker_num "$request_worker_num" \
           --arg ycsb_option "$ycsb_option" \
           '.workload = ($workload | tonumber) |
            .client_num = ($client_num | tonumber) |
            .request_worker_num = ($request_worker_num | tonumber) |
            .ycsb_option = ($ycsb_option | tonumber)' \
           src/common/config.json > src/common/config.json.tmp
        mv src/common/config.json.tmp src/common/config.json
    else
        # If jq is unavailable, use sed for simple replacement
        sed -i "s/\"workload\": [0-9]*/\"workload\": $workload/" src/common/config.json
        sed -i "s/\"client_num\": [0-9]*/\"client_num\": $client_num/" src/common/config.json
        sed -i "s/\"request_worker_num\": [0-9]*/\"request_worker_num\": $request_worker_num/" src/common/config.json
        if [ "$workload" = "1" ]; then
            sed -i "s/\"ycsb_option\": [0-9]*/\"ycsb_option\": $ycsb_option/" src/common/config.json
        fi
    fi

    log_info "Updated config.json: workload=$workload, client_num=$client_num, request_worker_num=$request_worker_num, ycsb_option=$ycsb_option"
}

# Function to modify log_enable in config.json
update_log_enable() {
    local log_enable=$1

    if [ "$log_enable" != "true" ] && [ "$log_enable" != "false" ]; then
        log_error "Invalid log_enable value: $log_enable"
        return 1
    fi

    if command -v jq &> /dev/null; then
        jq --argjson log_enable "$log_enable" \
           '.log_enable = $log_enable' \
           src/common/config.json > src/common/config.json.tmp
        mv src/common/config.json.tmp src/common/config.json
    else
        sed -i -E "s/\"log_enable\": (true|false)/\"log_enable\": $log_enable/" src/common/config.json
    fi

    log_info "Updated config.json: log_enable=$log_enable"
}

# Upload config.json to machines
upload_config() {
    log_info "Uploading config.json to all machines..."
    ./scripts/run.sh sync 8
    log_success "config.json upload complete"
}

# Function to run tests
run_test() {
    local workload_name=$1
    local client_count=$2
    local ycsb_option=${3:-0}
    local test_script="./test-${workload_name}.sh"

    if [ ! -f "$test_script" ]; then
        log_error "Test script does not exist: $test_script"
        return 1
    fi

    if [ "$workload_name" = "ycsb" ]; then
        log_info "Starting test: $test_script $ycsb_option $client_count"
        $test_script $ycsb_option $client_count
    else
        log_info "Starting test: $test_script $client_count"
        $test_script $client_count
    fi

    if [ $? -eq 0 ]; then
        log_success "Test complete: $test_script $client_count"
    else
        log_error "Test failed: $test_script $client_count"
        return 1
    fi
}

# Test one workload
test_workload() {
    local workload=$1
    local workload_name=$2
    local ycsb_option=${3:-0}

    log_info "=========================================="
    log_info "Starting test workload $workload ($workload_name)"
    if [ "$workload" = "1" ]; then
        log_info "YCSB option: $ycsb_option"
    fi
    log_info "=========================================="

    # Low-load test
    log_info "--- Low-load test ---"
    if [ "$workload" = "0" ]; then
        # append only: 1 client, request_worker_num=1
        update_config $workload 1 1 $ycsb_option

        log_info "Running low-load append test with log_enable=true"
        update_log_enable true
        upload_config
        log_info "Wait 2 seconds before starting test..."
        sleep 2
        run_test "append" 1

        log_info "Running low-load append test with log_enable=false"
        update_log_enable false
        upload_config
        log_info "Wait 2 seconds before starting test..."
        sleep 2
        run_test "append" 1
    else
        # Other workloads: 2 clients, request_worker_num=1
        update_config $workload 1 1 $ycsb_option
        upload_config
        log_info "Wait 2 seconds before starting test..."
        sleep 2
        run_test "$workload_name" 2 $ycsb_option
    fi

    # Full-load test
    log_info "--- Full-load test ---"
    local start end step
    case "$workload" in
        0|1)
            start=130; end=130; step=10
            ;;
        2)
            start=40; end=40; step=10
            ;;
        4)
            start=40; end=40; step=10
            ;;
        *)
            start=20; end=50; step=10
            ;;
    esac

    for client_count in $(seq $start $step $end); do
        local config_client_num=$((client_count / 2))
        log_info "Testing client count: $client_count (config client_num=$config_client_num, request_worker_num=2)"
        update_config $workload $config_client_num 2 $ycsb_option
        upload_config
        log_info "Wait 2 seconds before starting test..."
        sleep 2
        run_test "$workload_name" $client_count $ycsb_option
    done

    log_success "workload $workload ($workload_name) Test complete"
}

# Main function
main() {
    log_info "Starting all workload tests"

    # Back up the original config
    backup_config

    # Ensure config is restored when the script exits
    trap restore_config EXIT

    # Testworkload 0: append only
    test_workload 0 "append"

    # Testworkload 1: ycsb (4 options)
    for ycsb_option in 0 1 2 3; do
        test_workload 1 "ycsb" $ycsb_option
    done

    # Testworkload 2: lock
    test_workload 2 "lock"

    # Testworkload 4: eos/retwis
    test_workload 4 "retwis"

    # Test counter skewness sweep
    log_info "Starting test: ./test-skewness.sh"
    ./test-skewness.sh
    log_success "Test complete: ./test-skewness.sh"

    # Test YCSB-A segment sizes: Figure 12 uses 2 clients for latency and 130 for throughput.
    update_config 1 1 1 0
    log_info "Starting YCSB-A segment latency test: ./test-segment.sh 2"
    ./test-segment.sh 2
    log_success "YCSB-A segment latency test complete: ./test-segment.sh 2"

    update_config 1 65 2 0
    log_info "Starting YCSB-A segment throughput test: ./test-segment.sh 130"
    ./test-segment.sh 130
    log_success "YCSB-A segment throughput test complete: ./test-segment.sh 130"

    log_success "All workload tests complete!"
}

# Check dependencies
check_dependencies() {
    # Check whether required scripts exist
    local required_scripts=("scripts/run.sh" "test-append.sh" "test-ycsb.sh" "test-lock.sh" "test-retwis.sh" "test-skewness.sh" "test-segment.sh")

    for script in "${required_scripts[@]}"; do
        if [ ! -f "$script" ]; then
            log_error "Missing required script: $script"
            exit 1
        fi
    done

    # Check whether config.json exists
    if [ ! -f "src/common/config.json" ]; then
        log_error "Missing config file: src/common/config.json"
        exit 1
    fi

    log_success "All dependency checks passed"
}

# Show usage information
show_usage() {
    echo "Usage: $0 [Options]"
    echo ""
    echo "Options:"
    echo "  -h, --help     Show this help message"
    echo "  -c, --check    Check dependencies without running tests"
    echo ""
    echo "Test contents:"
    echo "  - workload 0: append only"
    echo "  - workload 1: ycsb (option 0,1,2,3)"
    echo "  - workload 2: lock"
    echo "  - workload 4: eos/retwis"
    echo "  - counter skewness sweep"
    echo "  - YCSB-A segment size sweep (latency: 2 clients; throughput: 130 clients)"
    echo ""
    echo "Each workload test:"
    echo "  - Low load: 1-2 clients (request_worker_num=1)"
    echo "  - Full load: 100~150(append/ycsb), 40~60(lock), 20~40(retwis) step10 (request_worker_num=2)"
}

# Parse command-line arguments
case "${1:-}" in
    -h|--help)
        show_usage
        exit 0
        ;;
    -c|--check)
        check_dependencies
        exit 0
        ;;
    "")
        # No arguments: run the full test suite
        check_dependencies
        main
        ;;
    *)
        log_error "Unknown argument: $1"
        show_usage
        exit 1
        ;;
esac
