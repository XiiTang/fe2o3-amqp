# Runtime integration additions

## Maintenance

This fork's `main` is the maintained Runtime integration line. Track the
original project through the read-only `upstream` remote, integrate selected
upstream changes here, and pin consumers to a tested full commit SHA.
Do not replace upstream history or float production dependencies on a branch.

The integration consolidates the 12 Runtime commits ending at
`2ac620c0eae3f34902263800fff7239545a58855` from `codex/runtime-engine`
onto the existing upstream-based `main` at
`8fd06bc36f2e3819ba4f01256b6688a2ce0d627a` (fe2o3-amqp 0.18.1).
The previous branch remains historical evidence, not a second maintenance line.

## Retained additions

- Bounded codec/message admission and shared partial-receive reservations.
- Bounded outgoing transfer retention and concurrent duplex progress.
- Session receive-window accounting tied to actual consumption.
- Native settlement observation, separate local/remote delivery state, and
  retained receiver progress across explicitly requested recovery.
- Joined connection/session shutdown and ownership-returning interrupted
  recovery, without inferred detach, disposition, reconnect, or retry.
- Explicit transaction lifetime without automatic rollback on drop.
- Original-frame observation without a second Runtime wire parser.

## Integration decisions

Preserve upstream 0.18.1's negotiated-frame-size splitting, cached frame limits,
relay-owned remote detach handling, link-stop error propagation, and boxed
detached endpoints. Refresh frame limits and stop reasons when switching
sessions, but preserve unsettled delivery state: an explicit resume must not
be turned into a fresh attach solely because its session changed.

A declared non-closing detach must not become a closing detach on endpoint
drop. Physical suspension retains only nonclosed endpoints whose parent
session stopped; queued peer-closing detach remains terminal.

Keep upstream's delivery-scoped negotiated message-size rejection distinct
from the native materialization budget (`MaterializationBoundExceeded`).
Adapt the upstream test peers to the Runtime receive-window control request
instead of removing their simultaneous-detach or frame-size assertions.

## Validation

Run from a configured Rust toolchain:

```sh
cargo test -p fe2o3-amqp --lib --features transaction,acceptor
cargo test -p fe2o3-amqp --features transaction,acceptor \
  --test large_message_split --test relay_remote_detach \
  --test clean_teardown --test link_stop_reason
```

The consolidated integration passes 83 library and 22 in-memory integration
tests on macOS arm64. These are not a replacement for the consumer's independent
broker, frozen-authentication, recovery, cancellation, or platform acceptance.

## Partial-transfer recovery

After a successful receiver Attach reconciliation, discard buffered partial
payload only when its delivery tag is no longer in the native unsettled map.
Releasing that buffer also returns its shared receive-budget reservation.
Keep both payload and reservation for deliveries still retained by the map,
including those absent from an incomplete peer map.

This follows AMQP 1.0 transport section 2.6.13 settlement reconciliation:
https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html

The defect was reproduced with an independent Artemis 2.57.0 broker: interrupt
a 128 KiB message after one 8192-byte Transfer, explicitly reconnect/resume,
then receive a complete redelivery after an empty complete peer unsettled map.
Previously the old prefix was prepended to the redelivery, corrupting encoding.
The regression checks exact binary payload, stale link-generation rejection,
and no delivery of partial data. A native unit test checks retained versus
settled prefixes and receive-budget release.

With this fix, 84 native library tests, 22 in-memory upstream integration
tests, and all 20 consumer AMQP 1.0 tests (including the independent interrupted
Artemis transfer) pass on macOS arm64.

## Settlement registration (2026-10-02)

Adapt #397 (6d64a5697408e4397e8b9ce9190e8560e0d5ae17) so send, resume, resend,
abort and outcome restatement register before session admission. Both normal
and tracked sends use the same registration guard and existing bounded byte
queue. Refusal/cancellation before completed handover withdraws registration;
a fully admitted transfer retains its outcome owner. Preserve frame splitting,
tracked dispatch and receive/materialization budgets. 93 library tests and the
22 selected in-memory lifecycle/splitting tests pass on macOS arm64.

#400 is a separate receive/resume semantics change, including public error and
link-detach behavior. It is not a replacement for the local materialization
budget or partial-transfer reconciliation, and is not included in this scoped
settlement repair. Evaluate its full receive-side contract before importing it.


## Receive/resume semantics and error scope (2026-10-03)

Semantically adapt upstream #400 (28c9d180fd08fced6a929cde2ee214013f369d63)
and #403 (682af8e2a5b9a8dddf5c0441e226cb7701c47fd2) onto this fork. Keep
strict encoded-section admission, the flat partial buffer and shared ReceiveBudget,
local/remote settlement separation, and explicit endpoint ownership/recovery.
The earlier #400 deferral above is superseded by this separately reviewed port.

Negotiated max-message-size and malformed continuation identities drive a closing
Detach on the affected link. Release partial storage on abort and size failure;
attribute tagless state continuations to their buffered delivery. Apply Received
prefix truncation before admitting replacement payload against message/storage
bounds. Ignore unknown resumed deliveries throughout their fragment sequence.
Sender aborts are implicitly settled and do not register a new outcome. Reallocate
the output handle between the mandatory detach/reattach cycles of resumption.

Shared materialization exhaustion remains a connection resource failure; it is
not mapped to message-size-exceeded or claimed to have completed a local Detach.
Preserve conservative handling of invalid native encoding/recovery positions.
ErrorRecovery is caller guidance, not proof of a completed handshake: is_closed
requires Closed plus a released output handle. close_in_place permits the owner
to retain a sender/receiver while a caller stops waiting. Neither API reconnects,
resumes or replays business data automatically.

120 transaction-enabled library tests and 18 in-memory lifecycle/splitting
tests pass on macOS arm64. Consumer wire tests
exercise oversize isolation on a shared session, delayed closing replies,
explicit detach completion after timeout and Execution-owned retirement. A peer
that continues using a detached handle can still incur a session protocol error;
this patch does not promise isolation from arbitrary later errant frames.
