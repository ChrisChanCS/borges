#!/bin/bash

# RDMA version of test-append.sh
# Memory node:   10.31.2.7 (ssh 10.31.1.157)   runs rdma_mem_server           (Rack A)
# Compute host:  10.31.2.1 (ssh 10.31.1.154)   runs sequencer + shard 0+1 + clients (Rack A)
#   -> both memory-access paths are same-rack .154->.157 = ~1.85us (low latency)
#
# Run this from the build machine (which has build/ + src/). Shard servers and clients
# run on the REMOTE shard host; client CSV results AND process logs are collected back
# here (result/rdma/ and ./logs/), the same way test-append.sh does.
#
# Usage: $0 <client_count>          # total clients; SERVER_ID = i % 2 (half per shard)
# NOTE:  config.json "client_num" is the per-shard connection barrier, so it must equal
#        client_count / 2 (e.g. client_count=40 -> client_num=20 = 20 clients per shard).

if [ $# -lt 1 ]; then
    echo "Usage: $0 <client_count>"
    exit 1
fi

CLIENT_COUNT=$1

# RDMA data-plane IPs (enp152s0np0) + SSH (management) IPs
# Low-latency layout: memory node + shard host both in Rack A (~1.85us apart).
# Sequencer is CO-LOCATED with the shard servers + clients on one machine (.154),
# so both hot paths (shard->mem and seq->mem) are the same-rack .154->.157 = ~1.85us.
MEM_NODE_RDMA="10.31.2.7";  MEM_NODE_SSH="10.31.1.157"   # memory node   (Rack A)
SEQ_NODE_RDMA="10.31.2.1";  SEQ_NODE_SSH="10.31.1.154"   # sequencer, co-located on the shard host (Rack A)
SHARD_NODE_SSH="10.31.1.154"          # runs shard 0+1, sequencer, and all clients (Rack A)
RDMA_PORT=9999
CORES0="0-4"; CORES1="5-9"            # 5 CPU cores per shard server (cgroup-equivalent)
LOGDIR="logs"                         # remote log dir (relative to ~), like test-append.sh
LOCAL_LOGS_DIR="./logs"               # where logs are collected on THIS machine

# Region size: ((1GB + 128MB) * 7) + 32KB root table  (matches default_cxl_mem_size)
REGION_SIZE=$((((1024*1024*1024 + 128*1024*1024) * 7) + 4096*8))

# timeout-bound every ssh so an intermittent connection stall can't hang the whole run
# (control commands return fast; nohup'd remote servers survive the ssh client being killed).
SSH="timeout 25 ssh -o StrictHostKeyChecking=accept-new -o ConnectTimeout=8 -o ServerAliveInterval=5 -o ServerAliveCountMax=3"
CSSH="timeout ${CLIENT_TIMEOUT:-900} ssh -o StrictHostKeyChecking=accept-new -o ConnectTimeout=8"  # long: blocks on client wait
SCP="timeout 120 scp -o StrictHostKeyChecking=accept-new -o ConnectTimeout=8 -q"

# Sanity: client_num (per-shard barrier) should be client_count / 2
CLIENT_NUM_CFG=$(grep -oE '"client_num"[[:space:]]*:[[:space:]]*[0-9]+' src/common/config.json | grep -oE '[0-9]+')
if [ -n "$CLIENT_NUM_CFG" ] && [ $((CLIENT_NUM_CFG * 2)) -ne "$CLIENT_COUNT" ]; then
    echo "WARNING: config.json client_num=$CLIENT_NUM_CFG but client_count=$CLIENT_COUNT."
    echo "         Each shard server waits for client_num connections; expected client_count=$((CLIENT_NUM_CFG*2))."
fi

cleanup_processes() {
    echo "Cleaning up all processes..."
    # -x (exact name) NOT -f: with -f the pattern matches the ssh shell itself and self-kills.
    $SSH $SEQ_NODE_SSH   "pkill -9 -x sequencer"        2>/dev/null || true
    $SSH $MEM_NODE_SSH   "pkill -9 -x rdma_mem_server"  2>/dev/null || true
    $SSH $SHARD_NODE_SSH "pkill -9 -x server; pkill -9 -x client" 2>/dev/null || true
    sleep 2
}

# ---- Clean files before running (following test-append.sh) ----
RESULT_DIR="./result/rdma/client_${CLIENT_COUNT}"
cleanup_processes                      # kill stale processes on all hosts
echo "Cleaning stale files before running..."
# Remote: fresh logs/ dir + remove stale client CSVs on every host
for h in $MEM_NODE_SSH $SEQ_NODE_SSH $SHARD_NODE_SSH; do
    $SSH $h "mkdir -p ~/$LOGDIR && rm -f ~/$LOGDIR/*" 2>/dev/null || true
done
$SSH $SHARD_NODE_SSH "rm -f ~/*.csv" 2>/dev/null || true
# Drop page cache on every host (like test-append.sh's run.sh init), best-effort
for h in $MEM_NODE_SSH $SEQ_NODE_SSH $SHARD_NODE_SSH; do
    $SSH $h "sudo sh -c 'echo 1 > /proc/sys/vm/drop_caches' 2>/dev/null; sync" 2>/dev/null || true
done
# Local: fresh result + logs dirs for this run
rm -rf "$RESULT_DIR" "$LOCAL_LOGS_DIR"
mkdir -p "$RESULT_DIR" "$LOCAL_LOGS_DIR"

# Sync binaries + config to their role hosts
echo "Syncing binaries..."
$SCP build/rdma_mem_server        $MEM_NODE_SSH:~/rdma_mem_server
$SCP build/sequencer              $SEQ_NODE_SSH:~/sequencer
$SCP src/common/config.json       $SEQ_NODE_SSH:~/config.json
$SCP build/server build/client    $SHARD_NODE_SSH:~/
$SCP src/common/config.json       $SHARD_NODE_SSH:~/config.json

# Start memory server
echo "Starting RDMA memory server on $MEM_NODE_SSH..."
$SSH $MEM_NODE_SSH "cd ~ && nohup ./rdma_mem_server --port $RDMA_PORT --size $REGION_SIZE </dev/null >$LOGDIR/rdma_mem_server.log 2>&1 & disown"
echo "Waiting for memory server to be ready..."
for i in $(seq 1 40); do
    sleep 0.5
    READY=$($SSH $MEM_NODE_SSH "grep -c 'Listening on port' ~/$LOGDIR/rdma_mem_server.log 2>/dev/null || echo 0")
    [ "$READY" -ge 1 ] 2>/dev/null && { echo "Memory server is listening."; break; }
done
echo "Memory server status:"; $SSH $MEM_NODE_SSH "tail -3 ~/$LOGDIR/rdma_mem_server.log"

# Start sequencer
echo "Starting sequencer on $SEQ_NODE_SSH..."
$SSH $SEQ_NODE_SSH "cd ~ && export RHODES_LOG_FILE=async_sequencer && nohup ./sequencer $MEM_NODE_RDMA $RDMA_PORT </dev/null >$LOGDIR/sequencer.log 2>&1 & disown"
echo "Waiting for sequencer to initialize..."
for i in $(seq 1 40); do
    sleep 0.5
    READY=$($SSH $SEQ_NODE_SSH "grep -c 'init thread pool' ~/$LOGDIR/sequencer.log 2>/dev/null || echo 0")
    [ "$READY" -ge 1 ] 2>/dev/null && { echo "Sequencer fully initialized."; break; }
    ALIVE=$($SSH $SEQ_NODE_SSH "pgrep -x sequencer >/dev/null 2>&1 && echo alive || echo dead" 2>/dev/null)
    if [ "$ALIVE" = "dead" ]; then
        echo "ERROR: sequencer crashed!"; $SSH $SEQ_NODE_SSH "tail -20 ~/$LOGDIR/sequencer.log" 2>/dev/null
        cleanup_processes; exit 1
    fi
done
echo "Sequencer status:"; $SSH $SEQ_NODE_SSH "tail -5 ~/$LOGDIR/sequencer.log"

# Verify memory server still alive
MEM_PID=$($SSH $MEM_NODE_SSH "pgrep -x rdma_mem_server" 2>/dev/null)
if [ -z "$MEM_PID" ]; then
    echo "ERROR: memory server died! Full log:"; $SSH $MEM_NODE_SSH "cat ~/$LOGDIR/rdma_mem_server.log" 2>/dev/null
    cleanup_processes; exit 1
fi
echo "Memory server PID=$MEM_PID is alive."

# Start shard servers on the shard host (5 cores each)
echo "Starting shard servers on $SHARD_NODE_SSH (cores $CORES0 / $CORES1)..."
$SSH $SHARD_NODE_SSH "cd ~ && \
    RHODES_LOG_FILE=async_server0 nohup taskset -c $CORES0 ./server 0 $MEM_NODE_RDMA $RDMA_PORT --listen_port=8081 </dev/null >$LOGDIR/server0.log 2>&1 & disown; \
    RHODES_LOG_FILE=async_server1 nohup taskset -c $CORES1 ./server 1 $MEM_NODE_RDMA $RDMA_PORT --listen_port=8082 </dev/null >$LOGDIR/server1.log 2>&1 & disown"
echo "Waiting for shard servers to accept connections..."
for i in $(seq 1 40); do
    sleep 0.5
    R=$($SSH $SHARD_NODE_SSH "grep -l 'accepting connections' ~/$LOGDIR/server0.log ~/$LOGDIR/server1.log 2>/dev/null | wc -l")
    [ "$R" = "2" ] && { echo "Both shard servers ready."; break; }
done
$SSH $SHARD_NODE_SSH "tail -3 ~/$LOGDIR/server0.log; echo ---; tail -3 ~/$LOGDIR/server1.log"

# Start clients on the shard host, wait for all to finish
echo "Starting $CLIENT_COUNT clients on $SHARD_NODE_SSH..."
$CSSH $SHARD_NODE_SSH "cd ~ && \
    for ((i=0; i<$CLIENT_COUNT; i++)); do
        RHODES_LOG_FILE=async_client_\$i ./client \$i \$((i % 2)) >$LOGDIR/client_\$i.log 2>&1 &
    done
    wait
    echo 'All clients finished.'"

# ---- Collect client CSV results back to THIS machine (like test-append.sh) ----
echo "Collecting client CSVs from $SHARD_NODE_SSH -> $RESULT_DIR ..."
$SCP "$SHARD_NODE_SSH:~/*.csv" "$RESULT_DIR/" 2>/dev/null || echo "  (no CSVs found on $SHARD_NODE_SSH)"
N_CSV=$(ls "$RESULT_DIR"/*.csv 2>/dev/null | wc -l)
echo "Collected $N_CSV CSV file(s) into $RESULT_DIR"
$SSH $SHARD_NODE_SSH "rm -f ~/*.csv" 2>/dev/null || true

# ---- Collect the spdlog [async_logger] logs -> borges_logs/-style files ----
# Async logs are file-based (like the CXL version): each co-located role writes its OWN file
# logs/async_<role>.log, separated by its differentiated RHODES_LOG_FILE name. Each file already
# holds only [async_logger] lines; the grep is a belt-and-braces filter. glog LOG(info) is on
# the separate raw-stream files (server0.log etc.) and is deliberately left out.
echo "Collecting [async_logger] logs from all hosts -> $LOCAL_LOGS_DIR ..."
mkdir -p "$LOCAL_LOGS_DIR"
$CSSH "$SEQ_NODE_SSH"   "grep -a '\[async_logger\]' ~/$LOGDIR/async_sequencer.log 2>/dev/null"      > "$LOCAL_LOGS_DIR/sequencer_log_" 2>/dev/null || true
$CSSH "$SHARD_NODE_SSH" "grep -a '\[async_logger\]' ~/$LOGDIR/async_server0.log 2>/dev/null"        > "$LOCAL_LOGS_DIR/server0_log_"   2>/dev/null || true
$CSSH "$SHARD_NODE_SSH" "grep -a '\[async_logger\]' ~/$LOGDIR/async_server1.log 2>/dev/null"        > "$LOCAL_LOGS_DIR/server1_log_"   2>/dev/null || true
$CSSH "$SHARD_NODE_SSH" "cat ~/$LOGDIR/async_client_*.log 2>/dev/null | grep -a '\[async_logger\]'" > "$LOCAL_LOGS_DIR/client_log_"    2>/dev/null || true
echo "Logs collected ([async_logger] only): $(ls "$LOCAL_LOGS_DIR" 2>/dev/null | wc -l) files in $LOCAL_LOGS_DIR"

# Aggregate throughput + latency from the collected CSVs
if [ "$N_CSV" -gt 0 ]; then
    echo "Aggregate:"
    python3 - "$RESULT_DIR" <<'PY'
import glob,sys
d=sys.argv[1]; rows=[]; st=[]; en=[]
for f in glob.glob(d+"/*.csv"):
    with open(f) as fh:
        next(fh,None)
        for ln in fh:
            p=ln.split(",")
            if len(p)>=3:
                try: st.append(int(p[0])); en.append(int(p[1])); rows.append(float(p[2]))
                except ValueError: pass
n=len(rows)
if n:
    rows.sort(); span=(max(en)-min(st))/1e6; thr=n/span if span>0 else 0
    q=lambda x: rows[min(n-1,int(n*x))]
    print(f"  files={len(glob.glob(d+'/*.csv'))} ops={n} window_s={span:.2f} throughput={thr:,.0f} ops/s")
    print(f"  latency_us mean={sum(rows)/n:.1f} p50={q(.5):.1f} p90={q(.9):.1f} p99={q(.99):.1f} p999={q(.999):.1f} max={rows[-1]:.1f}")
else:
    print("  NO DATA")
PY
fi

# Show a tail of the collected [async_logger] logs
echo "=== Sequencer [async_logger] tail ==="; tail -3 "$LOCAL_LOGS_DIR/sequencer_log_" 2>/dev/null
echo "=== Server 0 [async_logger] tail ===";  tail -3 "$LOCAL_LOGS_DIR/server0_log_" 2>/dev/null
echo "=== Client [async_logger] tail ===";    tail -3 "$LOCAL_LOGS_DIR/client_log_" 2>/dev/null

# Cleanup
cleanup_processes
echo "Done. Results in $RESULT_DIR ; logs in $LOCAL_LOGS_DIR"
