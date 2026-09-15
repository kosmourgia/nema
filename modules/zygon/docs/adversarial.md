# Adversarial protocol checks

```sh
PYTHONPATH=modules/zygon python -m unittest discover -s modules/zygon/tests -p test_protocol.py -v
```

Each test starts a real Unix registry and independent clients in a private
worktree-local `.nema/pt` directory. Synthetic fixture values contain no private
traffic. The short runtime path respects Linux's Unix socket path limit.

The suite checks identity ownership, unknown fields, nested incarnation-bearing
parent references, rollback after failed registration, connection takeover,
correlation duplicates/conflicts, result/cancellation races, PID reuse, disconnect
versus death, cooperative detach, deadlines, restart and actual SIGKILL recovery,
cursor/snapshot races, retained pagination, malformed/oversized frames, partial
frames, and unread responses alongside independently responsive control clients.

Assertions establish one retained local terminal disposition, including explicit
`outcome-unknown`. They make no exactly-once remote-effect, distributed lease,
causal total-order, or long-duration memory benchmark claim. Cursor ingestion
order never becomes an invented causal parent; observation sequence remains
producer-local. Replay does not resubmit invocations after a crash.

Verification after recovery of the rebooted guest is in progress.
