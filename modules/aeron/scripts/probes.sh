#!/usr/bin/env bash
set -euo pipefail
module=$(cd -- "$(dirname -- "$0")/.." && pwd)
mkdir -p "$module/.nema/evidence"
report=$(mktemp -d "$module/.nema/evidence/probes.XXXXXX")
cd "$report"
compiler=(java -XX:ActiveProcessorCount=2 -jar "${NEMA_FLIX_JAR:-/usr/share/java/flix/flix.jar}")
expect_rejected() {
    local name=$1 pattern=$2
    shift 2
    if "$@" >"$report/$name.log" 2>&1; then
        echo "Expected compiler rejection: $name" >&2
        exit 1
    fi
    rg -q "$pattern" "$report/$name.log"
}
mkdir -p "$report/scope/src"
cp "$module/probes/ScopeProbe.flix" "$report/scope/src/Main.flix"
cp "$module/probes/flix.toml" "$report/scope/flix.toml"
(
    cd "$report/scope"
    "${compiler[@]}" run --no-install --threads 2
) >"$report/abandoned-continuation.log" 2>&1
rg -q 'result=99; closeCalls=0' "$report/abandoned-continuation.log"
expect_rejected opaque 'opaque|Expected' "${compiler[@]}" check --no-install --threads 2 "$module/probes/OpaqueProbe.flix"
expect_rejected native-scope 'Escape' env NEMA_AERON_PROBE="$module/probes/NativeScopeEscape.flix" "$module/bin/aeron" check
expect_rejected coordinate 'StreamId|Position' env NEMA_AERON_PROBE="$module/probes/CoordinateMismatch.flix" "$module/bin/aeron" check
echo "FLIX_PROBES_PASS exact diagnostics: $report"
