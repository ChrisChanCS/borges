#! /bin/bash
#
# run_rdma.sh — RDMA counterpart to run.sh (COMPILE_SYNC) + test-append.sh (RUN/collect).
#
# The CXL flow (`run.sh COMPILE_SYNC 8`) compiles once and scp's the SAME binaries to
# 8 local QEMU VMs. RDMA is different: real physical hosts, and each host gets a
# DIFFERENT binary depending on its role. This script builds the RDMA binaries
# (use_rdma=true), uploads each to its role host, runs the append experiment, and —
# like test-append.sh — collects the client CSV results back to the local machine.
#
# Usage:
#   ./scripts/run_rdma.sh COMPILE               # build only (sets use_rdma=true first)
#   ./scripts/run_rdma.sh SYNC                  # upload existing build/ to the role hosts
#   ./scripts/run_rdma.sh COMPILE_SYNC          # build + upload
#   ./scripts/run_rdma.sh RUN <clients/shard>   # run experiment + collect CSVs to local
#   ./scripts/run_rdma.sh COLLECT <clients/shard># just pull CSVs from the shard host -> local
#   ./scripts/run_rdma.sh KILL                  # kill rhodes procs on all hosts
#
# Example (20 clients per shard server = 40 clients total):
#   ./scripts/run_rdma.sh COMPILE_SYNC
#   ./scripts/run_rdma.sh RUN 20
#   # -> results in result/append_rdma/client_40/*.csv on THIS machine
#
# Topology (override via environment variables if your hosts differ):
#   MEM_NODE     ssh host that runs rdma_mem_server        (default 10.31.1.158)
#   SEQ_NODE     ssh host that runs sequencer              (default 10.31.1.156)
#   SHARD_HOSTS  ssh host(s) running server(s)+client(s)   (default "10.31.1.154")
#                (SHARD_HOSTS[0] runs shard 0+1 and all clients)
#   MEM_RDMA     RDMA data-plane IP of the memory node     (default 10.31.2.3)
#   RDMA_PORT    memory-server TCP port                    (default 9999)
#   CORES0/CORES1 taskset cores for shard 0 / shard 1      (default 0-4 / 5-9)
#
set -uo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
REPO="$SCRIPT_DIR/.."
CONFIG="$REPO/src/common/config.json"

# Low-latency Rack A layout: memory node .157, sequencer co-located with shards on .154
MEM_NODE="${MEM_NODE:-10.31.1.157}"
SEQ_NODE="${SEQ_NODE:-10.31.1.154}"
read -r -a SHARD_HOSTS <<< "${SHARD_HOSTS:-10.31.1.154}"
SHARD_HOST="${SHARD_HOSTS[0]}"            # runs shard 0+1 and all clients
MEM_RDMA="${MEM_RDMA:-10.31.2.7}"
RDMA_PORT="${RDMA_PORT:-9999}"
CORES0="${CORES0:-0-4}"
CORES1="${CORES1:-5-9}"
RDST="${RDST:-}"                          # empty == remote home directory
REGION_SIZE=$((((1024*1024*1024 + 128*1024*1024) * 7) + 4096*8))
RESULT_ROOT="$REPO/result/append_rdma"

SSH_OPTS=(-o StrictHostKeyChecking=accept-new -o ConnectTimeout=8)
# bounded ssh — never hang more than ~25s on a stalled connection
BSSH()  { timeout 25 ssh "${SSH_OPTS[@]}" -o ServerAliveInterval=5 -o ServerAliveCountMax=3 "$1" "$2"; }
# long-running ssh — for the client `wait` (workload can take minutes)
CSSH()  { timeout "${CLIENT_TIMEOUT:-900}" ssh "${SSH_OPTS[@]}" "$1" "$2"; }
ssh_h() { ssh "${SSH_OPTS[@]}" "$1" "$2"; }
scp_h() { local dst="$1"; shift; scp "${SSH_OPTS[@]}" -q "$@" "${dst}:${RDST:-.}/"; }
RCD="cd ${RDST:-~} &&"                    # remote 'cd home/RDST'

usage() { sed -n '11,17p' "$0" | sed 's/^# \{0,1\}//'; }

ensure_rdma_config() {
    if grep -qE '"use_rdma"[[:space:]]*:[[:space:]]*false' "$CONFIG"; then
        echo "[run_rdma] setting use_rdma=true in config.json"
        sed -i -E 's/("use_rdma"[[:space:]]*:[[:space:]]*)false/\1true/' "$CONFIG"
    fi
    grep -qE '"use_rdma"[[:space:]]*:[[:space:]]*true' "$CONFIG" \
        || { echo "[run_rdma] ERROR: could not set use_rdma=true"; exit 1; }
}

compile() {
    ensure_rdma_config
    echo "[run_rdma] building RDMA binaries..."
    mkdir -p "$REPO/build"
    cmake -S "$REPO" -B "$REPO/build"
    cmake --build "$REPO/build" -j"$(nproc)"
    echo "[run_rdma] built: rdma_mem_server, sequencer, server, client"
}

sync() {
    local B="$REPO/build"
    for f in rdma_mem_server sequencer server client; do
        [ -x "$B/$f" ] || { echo "[run_rdma] ERROR: $B/$f missing — run COMPILE first"; exit 1; }
    done
    echo "[run_rdma] rdma_mem_server + config -> $MEM_NODE"
    ssh_h "$MEM_NODE" "mkdir -p ${RDST:-.}"; scp_h "$MEM_NODE" "$B/rdma_mem_server" "$CONFIG"
    echo "[run_rdma] sequencer + config       -> $SEQ_NODE"
    ssh_h "$SEQ_NODE" "mkdir -p ${RDST:-.}"; scp_h "$SEQ_NODE" "$B/sequencer" "$CONFIG"
    for h in "${SHARD_HOSTS[@]}"; do
        echo "[run_rdma] server + client + config -> $h"
        ssh_h "$h" "mkdir -p ${RDST:-.}"; scp_h "$h" "$B/server" "$B/client" "$CONFIG"
    done
    echo "[run_rdma] sync done."
}

kill_all() {
    for h in "$MEM_NODE" "$SEQ_NODE" "${SHARD_HOSTS[@]}"; do
        echo "[run_rdma] killing rhodes procs on $h"
        # -x (exact name), NOT -f, or pkill matches the ssh shell and self-kills.
        # SIGKILL: rdma_mem_server catches SIGTERM and does a slow ~8GB cleanup (keeps file
        # busy -> next scp fails with "Text file busy").
        BSSH "$h" '
            pkill -9 -x rdma_mem_server; pkill -9 -x sequencer
            pkill -9 -x server;          pkill -9 -x client
            for _ in $(seq 1 25); do
                pgrep -x "rdma_mem_server|sequencer|server|client" >/dev/null 2>&1 || break
                sleep 0.2
            done; true'
    done
}

# wait until `marker` appears in `logfile` on `host`, up to `n` checks
wait_marker() {
    local host=$1 logfile=$2 marker=$3 n=$4 i
    for ((i=1;i<=n;i++)); do
        BSSH "$host" "grep -q '$marker' $logfile 2>/dev/null" && { echo "[run_rdma]   ready: $host $logfile (~${i}s)"; return 0; }
        sleep 1
    done
    echo "[run_rdma] ERROR: $host did not reach '$marker' in $logfile"; return 1
}

start_pipeline() {
    echo "[run_rdma] starting memory node on $MEM_NODE"
    BSSH "$MEM_NODE" "$RCD nohup ./rdma_mem_server --port $RDMA_PORT --size $REGION_SIZE </dev/null >/tmp/rdma_server.log 2>&1 & disown; true"
    wait_marker "$MEM_NODE" /tmp/rdma_server.log 'Listening on port' 25 || exit 1

    echo "[run_rdma] starting sequencer on $SEQ_NODE"
    BSSH "$SEQ_NODE" "$RCD nohup ./sequencer $MEM_RDMA $RDMA_PORT </dev/null >/tmp/sequencer.log 2>&1 & disown; true"
    wait_marker "$SEQ_NODE" /tmp/sequencer.log 'init thread pool' 30 || exit 1

    echo "[run_rdma] starting shard servers on $SHARD_HOST (cores $CORES0 / $CORES1)"
    BSSH "$SHARD_HOST" "$RCD \
        nohup taskset -c $CORES0 ./server 0 $MEM_RDMA $RDMA_PORT --listen_port=8081 </dev/null >/tmp/server0.log 2>&1 & disown; \
        nohup taskset -c $CORES1 ./server 1 $MEM_RDMA $RDMA_PORT --listen_port=8082 </dev/null >/tmp/server1.log 2>&1 & disown; true"
    local i c
    for ((i=1;i<=30;i++)); do
        c=$(BSSH "$SHARD_HOST" "grep -l 'accepting connections' /tmp/server0.log /tmp/server1.log 2>/dev/null | wc -l" 2>/dev/null)
        [ "$c" = "2" ] && { echo "[run_rdma]   both shard servers ready (~${i}s)"; break; }
        sleep 1
    done
    [ "$c" = "2" ] || { echo "[run_rdma] ERROR: shard servers not ready"; exit 1; }
}

# set client_num (= clients per shard, the server-side connection barrier) and redeploy config
set_client_num() {
    local n=$1
    sed -i -E "s/(\"client_num\"[[:space:]]*:[[:space:]]*)[0-9]+/\1$n/" "$CONFIG"
    echo "[run_rdma] client_num=$n; redeploying config.json"
    for h in "$MEM_NODE" "$SEQ_NODE" "$SHARD_HOST"; do scp_h "$h" "$CONFIG"; done
}

run_clients() {
    local per=$1; local total=$((2*per))
    echo "[run_rdma] launching $total clients ($per per shard) on $SHARD_HOST ..."
    CSSH "$SHARD_HOST" "$RCD rm -f *.csv client_*.log
        for ((i=0;i<$total;i++)); do ./client \$i \$((i%2)) >client_\$i.log 2>&1 & done
        wait; echo ALL_CLIENTS_DONE"
}

# scp client CSVs from the shard host back to local result dir, then clean remote (like test-append.sh)
collect_results() {
    local total=$1; local dir="$RESULT_ROOT/client_${total}"
    mkdir -p "$dir"; rm -f "$dir"/*.csv
    echo "[run_rdma] collecting client CSVs from $SHARD_HOST -> $dir"
    scp "${SSH_OPTS[@]}" -q "$SHARD_HOST:${RDST:-.}/*.csv" "$dir/" 2>/dev/null \
        || echo "[run_rdma]   (no CSVs found on $SHARD_HOST)"
    local n; n=$(ls "$dir"/*.csv 2>/dev/null | wc -l)
    echo "[run_rdma] collected $n CSV file(s) into $dir"
    BSSH "$SHARD_HOST" "rm -f ${RDST:-.}/*.csv" >/dev/null 2>&1 || true
}

aggregate() {
    local total=$1; local dir="$RESULT_ROOT/client_${total}"
    python3 - "$dir" <<'PY'
import glob,sys
d=sys.argv[1]; rows=[]; st=[]; en=[]
files=glob.glob(d+"/*.csv")
for f in files:
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
    print(f"[run_rdma]   files={len(files)} ops={n} window_s={span:.2f} throughput={thr:,.0f} ops/s")
    print(f"[run_rdma]   latency_us mean={sum(rows)/n:.1f} p50={q(.5):.1f} p90={q(.9):.1f} p99={q(.99):.1f} p999={q(.999):.1f} max={rows[-1]:.1f}")
else:
    print("[run_rdma]   NO DATA")
PY
}

run_experiment() {
    local per="${1:-}"
    [[ "$per" =~ ^[0-9]+$ ]] || { echo "usage: $0 RUN <clients_per_shard>"; exit 1; }
    ensure_rdma_config
    kill_all
    set_client_num "$per"
    start_pipeline
    run_clients "$per"
    collect_results $((2*per))
    echo "[run_rdma] aggregate results:"; aggregate $((2*per))
    kill_all
    echo "[run_rdma] done. results in $RESULT_ROOT/client_$((2*per))/"
}

[ $# -ge 1 ] || { usage; exit 1; }
case "$1" in
    COMPILE)      compile ;;
    SYNC)         sync ;;
    COMPILE_SYNC) compile; sync ;;
    RUN)          run_experiment "${2:-}" ;;
    COLLECT)      [[ "${2:-}" =~ ^[0-9]+$ ]] || { echo "usage: $0 COLLECT <clients_per_shard>"; exit 1; }
                  collect_results $((2*${2})); echo "[run_rdma] aggregate:"; aggregate $((2*${2})) ;;
    KILL)         kill_all ;;
    *)            usage; exit 1 ;;
esac
