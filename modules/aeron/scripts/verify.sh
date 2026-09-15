#!/usr/bin/env bash
set -euo pipefail
module=$(cd -- "$(dirname -- "$0")/.." && pwd)
mkdir -p "$module/.nema/evidence"
report=$(mktemp -d "$module/.nema/evidence/verify.XXXXXX")
run_check() {
    local name=$1
    shift
    echo "CHECK $name"
    timeout 150s "$@" 2>&1 | tee "$report/$name.log"
}
run_check build "$module/bin/aeron" build
run_check flix "$module/bin/aeron" test
run_check transport "$module/bin/aeron" java nema.aeron.TransportChecks
run_check archive "$module/bin/aeron" java nema.aeron.ArchiveChecks
run_check reviewed-invariants "$module/bin/aeron" java nema.aeron.ReviewChecks
run_check sink "$module/bin/aeron" java nema.aeron.SinkChecks "$module/.nema"
run_check udp "$module/scripts/udp-interop.sh"
run_check handoff "$module/scripts/persistence-handoff.sh"
if [[ ${NEMA_AERON_MDC:-0} == 1 ]]; then
    run_check mdc "$module/bin/aeron" java nema.aeron.TransportChecks mdc
fi
echo "AERON_VERIFY_PASS reports: $report"
