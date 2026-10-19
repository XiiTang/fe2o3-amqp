# Runtime integration additions

## Maintenance

This fork's `boundless` branch carries the Runtime integration patches; `main`
mirrors upstream. Work on a short-lived branch from `boundless`, validate it,
then fast-forward `boundless` and pin consumers to a tested full commit SHA.
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
from the shared receive budget (`MaterializationBoundExceeded`).
Adapt the upstream test peers to the Runtime receive-window control request
instead of removing their simultaneous-detach or frame-size assertions.

## Validation

Run from a configured Rust toolchain:

```sh
cargo test -p fe2o3-amqp --lib --features transaction,acceptor
cargo test -p fe2o3-amqp --features transaction,acceptor \
  --test large_message_split --test relay_remote_detach \
  --test clean_teardown --test link_stop_reason --test detached_link_frames \
  --test engine_first_failure
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

## No fixed message ceiling (2026-10-03)

A delivery is bounded by the link's negotiated max-message-size, whose excess
detaches only that link, and its retained fragments by the shared
`ReceiveBudget`; the former fixed 16 MiB materialization ceiling is removed, as
upstream has none. It turned a message between that ceiling and the negotiated
size into a connection resource failure. `a_delivery_within_its_negotiated_size_and_budget_has_no_fixed_ceiling`
receives a 17 MiB delivery in three transfers and fails with the ceiling
restored; 124 transaction-enabled library tests pass on macOS arm64.

## A receiver's own close survives a dropped `recv` (2026-10-10)

`Receiver::recv` documents itself as cancel-safe (#22), but the closes it starts
itself — an oversized delivery (#400), a malformed delivery, the transactional
acquisition refusal, which carried a `FIXME: cancel safe?` — awaited the peer's
answering Detach inside `recv`. A caller whose `tokio::select!` dropped `recv`
after the closing Detach went out lost the cause: the next `recv` read the
peer's answer in `recv_inner` as a Detach the peer sent on its own and returned
`LinkStateError::RemoteClosed`. Against upstream `main` 682af8e2 a test that
drops the first `recv` after its closing Detach and answers it gets
`RemoteClosed` from the second `recv` instead of `MessageSizeExceeded`.
Boundless's AMQP 1.0 receiver actor selects over `recv` this way and so
reported an oversized delivery as the peer closing the link.

The receiver now records the close — the error its Detach carries and the
error `recv` returns — in `self_close` before sending the Detach, and every
self-close goes through `close_itself`. `on_incoming_detach` is the one place
a Detach the link receives is told apart: one arriving while its own close is
CloseSent completes the close (a non-closing answer reattaches and closes, AMQP
1.0 §2.6.6) and returns the recorded cause; any other stays the peer's.
`recv_inner` resumes a recorded close before reading anything else, sending
the Detach first if a dropped `recv` never queued it.

`oversized_delivery_close_resumes_after_recv_is_dropped_mid_close`,
`..._before_its_detach_is_sent`, `malformed_delivery_close_resumes_after_recv_is_dropped_mid_close`
and `a_detach_the_peer_sends_on_its_own_is_reported_as_the_peers` cover it;
the first is the upstream reproduction's scenario. 125 transaction-enabled,
121 default and 128 transaction+acceptor library tests and the
`large_message_split`, `relay_remote_detach`, `clean_teardown` and
`link_stop_reason` integration tests pass on macOS arm64. Upstream issue: to be
filed against minghuaw/fe2o3-amqp with the reproduction above.


## Detach and End state follow queue admission (2026-10-15)

`Link::send_detach` is the shared check point for sender/receiver detach and
close. Return its enqueue error before advancing link state, publishing dispatch
or consuming the output handle, as `send_attach_inner` already does. Preserve
the recorded session-stop cause. A queued non-closing Detach retains its handle;
a queued closing Detach consumes it. Neither can be committed a second time.

`rejected_detach_preserves_state_handle_and_session_stop_reason` fails at the
preceding pin and passes here. It covers both closing modes, with and without a
recorded session-stop cause. `accepted_detach_commits_one_frame_and_one_state_transition`
checks the accepted frame, dispatch flag, handle ownership and repeated-send
refusal. `cargo +1.97.0 test -p fe2o3-amqp --lib --locked --features
transaction,acceptor link::` passes all 97 matching tests on macOS arm64. The
ignored local test lock resolves tokio-util 0.7.19, matching Boundless's lock;
unconstrained 0.7.20 has an unrelated transport Sink inference failure.

`Session::send_end` broke the same rule: it set EndSent, Discarding or Unmapped
before sending End, so a refused End left the session claiming it had sent one.
It now computes the next state, returns the connection-stop error if the End is
not queued, and only then advances. Every session, acceptor and transaction End
delegates to it. `rejected_end_keeps_its_state_and_accepted_end_commits_once`
fails without the change and passes here, for Mapped with and without an error
and for EndReceived. `cargo +1.97.0 test -p fe2o3-amqp --lib --locked --features
transaction,acceptor` passes on macOS arm64.


## Retained Clippy cleanup and inline transport shapes (#63)

The non-boxing cleanup from `47d6c83f` remains: remove the unused IntoReader
abstraction, bound, implementation and reader-only test (receive decoding uses
AsByteIterator); simplify the allocator result match and coordinator close
logging, and pass the owned test payload without a needless borrow.

The boxing parts of that commit are retired. Sender and Receiver again own
inline private inner state; only their existing detached forms allocate.
Builders, acceptors and detach/resume callers use that representation. Transfer
queue errors again carry the unsent LinkFrame inline, preserving Full/Closed,
and DuplexEvent carries the incoming frame without a per-frame Box allocation;
the connection engine consumes it directly.

Strict Clippy keeps five narrow, reasoned allowances: `result_large_err` on
Sender::into_detached and Receiver::into_detached (return the original endpoint
when it cannot detach) and transfer_queue::Sender::try_send (return the unsent
frame), and `large_enum_variant` on DuplexEvent and acceptor::LinkEndpoint.
This follows upstream 0.18.1's existing SessionOutgoingItem allowance and its
stated policy of keeping the common frame path inline rather than allocating
only to even out variant sizes. No crate-wide lint level changes.

Validation on macOS arm64 / Rust 1.97.0: the unboxed shapes without these
allowances produce exactly those five diagnostics (strict Clippy exit 101);
with them `cargo clippy --locked -p fe2o3-amqp --all-targets --features
transaction,acceptor -- -D warnings` passes (exit 0). Full workspace tests use
`cargo test --workspace --locked --features transaction,acceptor`, including
existing link detach/resume, queue, duplex-progress and engine-end regressions.
These are representation changes; no test maintains a removed boxed shape.


## One engine end through stop and join (#63, batch 2)

ConnectionHandle and SessionHandle now return their engine's domain result from
`stop_and_join(&mut self) -> Result<(), Error>`. The private EngineOutcome is the
one checkpoint for both engines: its atomic winner distinguishes natural end
from owner stop, keeps a first-probe result across a cancelled waiter, joins the
task and probes the outcome again before reporting it. Native and local spawn
paths, client and acceptor engines, all use the same sender/receiver pair. Only
an owner-stop cancellation becomes Ok; an unpublished task failure keeps its
JoinError (now also represented in the session error enum). Repeated calls after
reading an end return Ok. No new public method or protocol cleanup is introduced.

Regression: `cargo test --locked -p fe2o3-amqp --lib --features transaction,acceptor local_stop_tests`.
The original fork fails both published-natural-result cases (exit 101); the patch
passes all 8 (exit 0), including connection/session barrier races where a natural
end arrives after the first probe and before abort, task panic and repeated stop.
Removing the post-join probe also makes both barrier regressions fail (exit 101).
`cargo test --locked -p fe2o3-amqp --lib --features transaction,acceptor` passes
136 tests (the unused reader-only test was removed by the lint prerequisite);
the clean_teardown, link_stop_reason, relay_remote_detach and
large_message_split integration targets pass 22 tests (all exit 0), on macOS arm64.
`cargo check --locked -p fe2o3-amqp --features acceptor` passes (exit 0), covering
the non-transaction acceptor path as well. Tokio's atomic completion/cancellation
and oneshot result ownership were inspected before selecting this mechanism.
The original strict-Clippy prerequisite was separate from this engine-end
change; its boxing is now retired under the inline-shape policy above.
The outcome sender retains Debug support required by the existing tracing spans.
`cargo check --locked -p fe2o3-amqp --features transaction,acceptor,tracing,log`
failed without its Debug derive (exit 101); the derive restores that build
(exit 0).

## A delivery takes link credit as its first frame enters the session (#72)

A sender took one link credit (advancing `delivery_count`) before waiting for
the session's transfer queue, whose retained-bytes bound and slots are both
cancellation points and which refuses a frame larger than it retains. A send
cut short or refused there kept the credit, and the receiver's next flow never
gives it back: the sender computes `delivery-count_rcv + link-credit_rcv -
delivery-count_snd`, and the receiver never counted a delivery it did not get.
A receiver granting one credit at a time then stalls the link for good. The
transaction controller alone refunded it, through a drop guard.

`SenderLink::send_delivery` is now the one place a delivery takes credit. It
waits for credit (`SenderFlowState::credit_ready`, which registers its waiter
before it checks, so a flow between the two wakes it), then reserves the first
frame's room (`transfer_queue::Sender::reserve`, retained bytes then a slot),
holding neither while it waits for the other; then, in one synchronous step,
it takes the credit (`try_consume`), gives the frame the delivery tag the
credit gives, registers the unsettled delivery and hands the frame over. A
drain that took the credit meanwhile sends it back to waiting. `send_payload`,
`resend` and the transaction controller send through it; the controller's
`ControlCredit` refund, the async `Consume` trait and the unused
`send_payload_with_transfer` paths are gone. qpid-proton (`pni_advance_sender`)
and azure-amqp (`TrySendDelivery`) take credit as a delivery enters the
session in the same way.

`cargo +1.97.0 test -p fe2o3-amqp --lib --locked --features
transaction,acceptor link::sender::tests`:
`a_send_cut_short_before_the_queue_takes_it_keeps_its_credit`,
`a_frame_the_queue_refuses_keeps_its_credit` and
`a_send_to_an_ended_session_keeps_its_credit` leave `(link_credit,
delivery_count)` at `(0, 1)` instead of `(1, 0)` on the previous commit;
`a_delivery_is_registered_when_its_first_frame_reaches_the_session` keeps the
settlement registration rule. The library (135) and the four in-memory
integration suites under Validation pass on macOS arm64.

## A frame is split to what the session queue retains (Boundless #86)

The bounded transfer queue (Retained additions) retains at most 32 MiB of
frames and refused one whose payload plus its 1 KiB allowance exceeded that,
while `split_transfer` split a delivery only by the peer's max-frame-size,
`u32::MAX` when the peer omits it. A message the link's max-message-size
admits but larger than about 32 MiB went out as one frame and its send
failed (`IllegalState`, the stop error of a session that had not stopped).
qpid-proton (`transport.c` `pni_process_tpwork_sender`) and rhea
(`session.js`) also split by the peer's frame size alone, but neither has a
bounded queue.

`split_transfer` is the one place a delivery is cut into frames, for new,
resumed, restated, aborted and resent deliveries alike; it now bounds each
frame's payload by the smaller of the peer's max-frame-size and
`transfer_queue::MAX_FRAME_PAYLOAD`, which the queue derives from its own
bound. The queue's size refusal becomes a debug assertion, kept as a refusal
in release builds because a frame larger than the semaphore could never be
admitted.

`a_message_larger_than_the_session_retains_per_frame_round_trips`
(`large_message_split`) sends 33 MiB to an in-memory listener whose Open
omits max-frame-size (checked on the wire) and fails at the preceding pin
with `IllegalState`. `a_delivery_is_split_to_what_the_peer_and_the_session_queue_take`
replaces `a_frame_the_queue_refuses_keeps_its_credit`, which asserted the
refusal: the queue bound exactly, one byte over, two frames over and a smaller
peer frame size, each taking one credit. 135 transaction+acceptor library
tests and the four Validation integration suites (23 tests) pass, and strict
Clippy passes, on macOS arm64 / Rust 1.97.0.


## A detached attachment's frames stop at the session's reply (Boundless #87)

Once the session answered a peer's detach, the link's frames for that
attachment still reached the wire after the reply: a transfer already in the
session queue, one waiting for the peer's session window, a link flow, and a
local detach, which on a handle a later attachment reused also released that
attachment. A delivery registered after the relay failed a closed link's
pending ones waited for an outcome that could no longer come, and a peer's
disposition for the detached link's delivery-id settled the delivery under
the same tag of the next link on the handle.

Each output-handle allocation now carries an `AttachmentId`, process-wide and
never reused, and `LinkFrame::Transfer`, `Flow` and `Detach` carry the id of
the attachment they were made for. `Session::is_current_output_attachment` is
the one check: a transfer is checked before it is buffered and again before it
takes a transfer-id, and a link flow and a local detach are checked as they
go out. A frame of an ended attachment goes nowhere, takes no transfer-id,
leaves the peer's window and the session's maps as they were, and releases its
queue room; ending an attachment drops its transfers still waiting for the
window. The session drops a detached link's dispositions routes in the step
that removes its relay, as qpid-proton drops a released link's deliveries from
its session (`c/src/core/engine.c`, `pn_link_free` and `pn_delivery_map_del`);
the link keeps its own unsettled deliveries for a resume.

A sender's unsettled store closes for good under its lock, keeping the first
close error, and the relay closes it before the forward that can wait for the
link's channel. Every registration goes through `Store::open`, so
`send_delivery`'s one step that takes credit and registers either sees the
close and returns its error, keeping the credit, or registers a delivery the
close fails. Every wait for the session queue, for a new delivery's first
frame, its later frames and a resumed, restated, aborted or resent delivery,
ends at the peer's detach. A pause before the first frame is handed over
returns `RemoteDetached` and keeps the credit; a pause after it keeps the
whole payload and registration and returns the pending settlement, for an
explicit resume to send again (AMQP 1.0 §2.6.13: a peer without the tag gets
it as a new delivery, one holding it with a null state gets it whole under
its tag with `resume` set and no new credit). A resume keeps each local
delivery in the store until its step has been handed over, rather than taking
them out first, so a resume cut short leaves the rest for the next resume or
the close; this refines Settlement registration above, where a resume
registered its deliveries again before admission. A resend that moved a
delivery to its new tag and is then cut short, as a resume's interrupt drops
it, keeps that delivery under the new tag with its outcome: only a delivery
new to the store is withdrawn when its send is cut short.

`cargo +1.97.0 test -p fe2o3-amqp --locked --features transaction,acceptor`:
`session::engine::detached_attachment_tests` processes the peer's detach and
then an old transfer, flow or local detach, directly, after a zero-window
replay, in the drain before End and with the handle reused, and fails on any
of those with the check reduced to the handle. `link::sender::tests`
`a_detach_ready_with_the_queue_room_stops_the_hand_over` (first frame, later
fragment, resume), `the_store_orders_a_registration_with_the_relay_close` and
`credit_drained_while_the_first_frame_waits_for_room_is_not_taken` cover the
admission races, and
`a_resend_cut_short_after_its_first_frame_keeps_the_delivery` the dropped
resend, whose delivery and outcome were withdrawn before.
`tests/detached_link_frames.rs` scripts the peer over an in-memory stream and
stops and joins the client's engines at the end of each test: a close while a
transfer waits for the window, three pause-and-resume cases, and the peer's
disposition, as receiver and as sender, for a detached link's delivery-id
after its handle was reused. On the preceding pin four of the first five
fail: the close and the never-written pause each see an old transfer after the
reply, the later-fragment pause stalls, and the next link's delivery settles
with the detached link's outcome; the resume under the original tag already
took no credit. With the routes kept, the sender's disposition removes the
next receiver's retained delivery. The library (140) and the in-memory
integration suites under Validation with `detached_link_frames` pass, and
strict Clippy passes, on macOS arm64 / Rust 1.97.0.

## An engine's first failure is how it ended (Boundless, AMQP 1.0 stop reasons)

A connection engine that met a protocol violation, a frame illegal in its
state, a session the peer opens or a frame for no session, answered it with
a Close and waited for the peer's; when the peer ended its stream instead,
the end the wait met replaced the violation as the engine's result, so the
owner saw only the end of a stream, which Boundless reports as the peer's
normal end. An opening that failed lost its failure the same way to its
Close. Upstream 0.18.1 has both.

`event_loop` keeps its first failure where it meets what `on_error`'s
cleanup returns, for every failure that cleanup handles, and the opening
keeps its failure over its Close's. An incoming stream that ends while the
connection is open is now its own first failure, `IncomingClosed`: it gets
the Close an illegal state gets, as before, and reports as the stream's end
(`Error::TransportError(Io(UnexpectedEof))`), so an owner can still tell a
stream that ended on its own from a violation before it, as qpid-proton
keeps a transport's first error condition.

`tests/engine_first_failure.rs` scripts the peer over an in-memory stream: a
second Open, a Begin that opens a session and an End for no session, each
followed by the end of the stream, end the engine with that failure after
the client sent the matching Close; an Open answered by a Begin fails the
opening with `IllegalState`; a stream that ends on its own still ends with
`UnexpectedEof` after the client's Close (amqp:illegal-state). On the
preceding pin the four failure cases fail, ending with the "Expecting remote
close" end of the stream instead. The library and the in-memory integration
suites under Validation pass, and strict Clippy passes, on macOS arm64 /
Rust 1.97.0.
