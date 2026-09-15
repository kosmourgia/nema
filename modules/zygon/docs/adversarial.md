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

Verification after recovery of the rebooted guest:

```text
Ran 23 tests in 1.278s
OK
```

This includes actual SIGKILL/restart, owner rollback after a rejected large
registration, and pending uncertainty when a connection is replaced with a
valid continuity token.

A subsequent live-Flix provider-disconnect test exposed a callback shutdown bug:
`Client.close()` included its own current callback in cancellation and join.
The exact observed failure was `RecursionError: maximum recursion depth exceeded`
in `asyncio/tasks.py:761`, with `child.cancel(msg=msg)` recursively awaiting its
own gathering future. The fix excludes the current worker. A 24th deterministic
regression, `test_provider_can_close_from_its_own_callback_without_self_join`,
checks that callback shutdown returns and the original invocation becomes
retained `outcome-unknown`. All three live-Flix tests then passed in 15.548s.
