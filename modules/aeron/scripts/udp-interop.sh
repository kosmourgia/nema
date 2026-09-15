#!/usr/bin/env bash
set -euo pipefail
module=$(cd -- "$(dirname -- "$0")/.." && pwd)
state="$module/.nema"
mkdir -p "$state/runs"
run=$(mktemp -d "$state/runs/interop.XXXXXX")
export NEMA_AERON_RUN="$run"
peer_pids=()
cleanup() {
    local pid
    for pid in "${peer_pids[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then kill "$pid" 2>/dev/null || true; fi
    done
    for pid in "${peer_pids[@]}"; do wait "$pid" 2>/dev/null || true; done
}
on_exit() {
    local result=$?
    cleanup
    if (( result != 0 )); then echo "UDP interop failed; logs: $run" >&2; fi
}
trap on_exit EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
wait_peer() {
    local target=$1 pid result=0
    local remaining=()
    wait "$target" || result=$?
    for pid in "${peer_pids[@]}"; do
        if [[ "$pid" != "$target" ]]; then remaining+=("$pid"); fi
    done
    peer_pids=("${remaining[@]}")
    return "$result"
}

# Compile once before launching a peer with a bounded connection deadline.
"$module/bin/aeron" check >"$run/check.log" 2>&1
mkdir -p "$run/native-classes"
javac -cp "$state/deps/aeron-all-1.53.1.jar" -d "$run/native-classes" \
    "$module/java/nema/aeron/NativePeer.java" "$module/java/nema/aeron/InteropSupport.java"
java_cmd=(java --add-opens java.base/jdk.internal.misc=ALL-UNNAMED
    --sun-misc-unsafe-memory-access=allow -XX:ActiveProcessorCount=2
    -cp "$state/deps/aeron-all-1.53.1.jar:$run/native-classes")
hex=$("${java_cmd[@]}" nema.aeron.InteropSupport)
wait_ready() {
    local file=$1 pid=$2
    local deadline=$((SECONDS + 45))
    while [[ ! -s "$file" ]]; do
        if ! kill -0 "$pid" 2>/dev/null || (( SECONDS >= deadline )); then
            echo "peer did not become ready: $file; logs: $run" >&2
            return 1
        fi
        sleep 0.02
    done
}

# Flix owns its driver; native receiver owns another, bound by the OS to port 0.
NEMA_AERON_INTEROP_DRIVER="$run/flix-send-driver" \
NEMA_AERON_INTEROP_READY="$run/flix-send.ready" \
NEMA_AERON_INTEROP_PEER="$run/native-receive.ready" \
    timeout 60s "$module/bin/aeron" run --entrypoint Nema.Aeron.Interop.sendMain >"$run/flix-send.log" 2>&1 &
flix_pid=$!
peer_pids+=("$flix_pid")
wait_ready "$run/flix-send.ready" "$flix_pid"
"${java_cmd[@]}" nema.aeron.NativePeer receive-owned "$run/native-receive-driver" \
    'aeron:udp?endpoint=127.0.0.1:0' 7101 "$hex" 6 "$run/native-receive.ready" >"$run/native-receive.log" 2>&1 &
native_pid=$!
peer_pids+=("$native_pid")
wait_peer "$native_pid"
wait_peer "$flix_pid"

# Reverse direction, two independent native producer processes and sessions.
NEMA_AERON_INTEROP_DRIVER="$run/flix-receive-driver" \
NEMA_AERON_INTEROP_READY="$run/flix-receive.ready" \
    timeout 60s "$module/bin/aeron" run --entrypoint Nema.Aeron.Interop.receiveMain >"$run/flix-receive.log" 2>&1 &
flix_pid=$!
peer_pids+=("$flix_pid")
wait_ready "$run/flix-receive.ready" "$flix_pid"
channel=$(<"$run/flix-receive.ready")
for session in 1 2; do
    "${java_cmd[@]}" nema.aeron.NativePeer send-owned "$run/native-send-$session-driver" \
        "$channel" 7101 "$hex" 3 >"$run/native-send-$session.log" 2>&1 &
    native_pid=$!
    peer_pids+=("$native_pid")
    wait_peer "$native_pid"
done
wait_peer "$flix_pid"
rg 'FLIX_SENT|FLIX_RECEIVED|NATIVE_SENT|NATIVE_RECEIVED' "$run" -g '*.log'
echo "UDP_INTEROP_PASS dynamic endpoints, independent drivers/processes, both directions, two sessions, binary+UTF-8+fragmentation; logs: $run"
