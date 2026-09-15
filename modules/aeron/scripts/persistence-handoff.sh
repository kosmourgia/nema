#!/usr/bin/env bash
set -euo pipefail
module=$(cd -- "$(dirname -- "$0")/.." && pwd)
mkdir -p "$module/.nema/runs"
export NEMA_AERON_RUN=$(mktemp -d "$module/.nema/runs/handoff.XXXXXX")
echo "handoff runtime: $NEMA_AERON_RUN"
set +e
NEMA_AERON_PHASE=prepare "$module/bin/aeron" run --entrypoint Nema.Aeron.Handoff.main 2>&1 | tee "$NEMA_AERON_RUN/prepare.log"
status=${PIPESTATUS[0]}
set -e
[[ $status == 73 ]] || { echo "Expected post-commit crash exit 73, got $status" >&2; exit 1; }
NEMA_AERON_PHASE=recover "$module/bin/aeron" run --entrypoint Nema.Aeron.Handoff.main 2>&1 | tee "$NEMA_AERON_RUN/recover.log"
echo "retained Archive, sink journal, and locators: $NEMA_AERON_RUN"
