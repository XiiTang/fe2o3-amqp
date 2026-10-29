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
  recovery; after an admitted Attach, upstream's nonclosing Detach cleanup
  completes before custody returns. No inferred disposition, reconnect or retry.
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


## Exact suspended/crossed link end exchanges and tracked coordinator Detach completion (Boundless W1-D1)

The host must correlate every internal Attach used to finish an AMQP 1.0
§2.6.6 crossed end, retain that correlation until actual exit, and preserve
which exchange failed. The ordinary recovery Attach path also reconciles local unsettled entries;
a close-only reattach must not replay or reconcile business deliveries.
Both roles keep one outgoing Attach implementation and one validation body,
selected by private `AttachPurpose::{Ordinary,CloseOnly}`. The borrowed final
`purpose` argument to `send_attach_inner` leaves ordinary behavior unchanged;
CloseOnly advertises null unsettled and may clear the local native store,
validates terminus/role/handle/settle fields, and rejects incomplete
reconciliation rather than replaying a delivery. This extends the earlier
partial-transfer and detached-attachment patches above.

The public sender entries are `close_in_place_exact<F,G,E>(&mut self,
peer_suspended:bool,before:F)` and `detach_borrowed_exact<F,G,E>(&mut self,
before:F)`, returning `Result<(),LinkExchangeError<SenderAttachError,E>>`;
the receiver has the corresponding exact close and the receive/suspension
entries below. The callback is `FnMut()->Result<G,E>+Send`, with G/E Send;
it runs before reallocation and its guard stays owned through actual closing
reply or failure. `LinkExchangeError<A,E>` retains Attach, Detach and
Correlation phases, with phase-only Display and typed sources. Only the shared
`close_exact` is pub(crate), for the transaction module; other shared exact
helpers remain pub(super). `Controller::close_retained_tracked_exact` takes
`&self`, the original `Arc<AtomicBool>` dispatch flag and the same callback,
returns the sender phase error, and holds its existing native inner mutex
through the exchange. It performs no business resume or Discharge.

The public receiver `recv_detaching_exact<T,O,F,G,E>(&mut self,&mut O,&mut F)`
returns `Result<ReceiverDetachStep<T>,ReceiverDetachError<E>>`. Its Delivery,
SelfClosing, Suspended and Closed steps keep normal Transfer and partial-body
processing during suspension; no generic frame-discarding wait is used there.
`recv_collecting_stopped<T,O>(&mut self,&mut O)` checks the original native
session stop before any queue movement, drains through that same parser and
returns Delivery/Drained/Closed or the actual receive fault without new wire
work. `recv_observed` signals SelfClosing before sending a protocol close;
`self_close_cause` borrows its saved cause and `finish_self_close_exact` consumes
the exact close handshake with the scoped callback. Legacy recv preserves its
contract. Existing suspended same-session dispositions remain unchanged.

A dispatched tracked Declare/Discharge must also finish if its control link
is detached without closing. `SharedTransaction::{declare_tracked,
discharge_tracked}` use the existing bodies with a construction-bound public
`ControlTracker::new(Arc<dyn ControlProbe>)`; ordinary entries pass None.
Its opaque ControlAttachment/ControlReceipt retain the actual attachment,
final-fragment admission fence, fresh Detach sequence and actual terminal
answer. Original command identity and causal classification belong to the
host; there is no duplicate native request ordinal, tag or first-fragment fence. The private immutable receipt
payload is boxed, with read-only getters. Probe calls run outside its snapshot
mutex. Native send registers unsettled state before first admission, and
writes terminal answered state before notifying the original oneshot in both
settlement paths; failure writes no invented answer. Only a tracked fragmented
send returns its consumed Detach through the control-only completion path,
including actual closed/error and an incomplete receipt. Ordinary None send
and resume keep their pending-settlement behavior. `ControlDetached` preserves
these facts without retrying, replaying or implicitly rolling back anything;
the host remains the sole command-causality classifier.

Regression fixtures at source revision
`3455283d7b90c1e12817cffa05b83951a278df47` include nonempty local stores for both
roles, already-suspended and crossed close, complete null/nonempty peer maps,
incomplete/terminus/allocation/callback failures, in-flight complete and partial
receives during detach, checked stopped drain, protocol faults before wire,
exact current native disposition identity, Declare/Discharge with both closed
flags, genuinely fragmented controls and answer-before-oneshot gates. The
controller regression invokes its public retained exact close. Native gates
and queues force the interleavings; no sleeping supplies an assertion.

### Receiver Close preserves the admitted prefix (necessity correction)

`Receiver::close_in_place_exact<T,O,F,G,E>(&mut self,peer_suspended:bool,
observer:&mut O,before:&mut F)` returns the same
`Result<ReceiverDetachStep<T>,ReceiverDetachError<E>>` and parser bounds as
`recv_detaching_exact`. Active Close sends closed=true once, then returns each
complete Delivery through the original attachment before consuming its first
native Detach. In receiver CloseSent the existing output_handle remains held;
`on_complete_transfer` accepts that truthful state and constructs the Delivery
with that original handle. Sender/controller release behavior is unchanged.
First-Detach application releases the handle. Observed-but-unconsumed suspension
first drains this same prefix without another outgoing Detach and returns
Suspended. Only after that boundary may CloseOnly use the shared discard wait;
it allocates no new receiving admission. Faults remain errors, not empty success.

Regression `exact_close_preserves_the_original_receiving_prefix` holds the peer
Detach until complete and fragmented deliveries have escaped the actual Close
parser, checks their bodies, original handle and the actual outgoing close, then
finishes closing, crossed suspension and already-observed suspension arms. It
also admits an incomplete trailing delivery. The sender's nonempty-store fixture
checks the actual scoped null-map close without preserving store/oneshot identity.

## Receive admission verdict and per-Transfer semantic progress (Boundless W1-D1)

The old unit observation hook could not refuse forwarding, and Transfers that
abort, are ignored on resume, or only update retained delivery state yield no
Delivery with which the host could retire a provisional receipt.
`IncomingFrameObserver::incoming(&self,Arc<IncomingFrame>)->IncomingFrameAction`
now returns Forward or RefuseTransfer. Transport checks that verdict before
forwarding: only it can construct the opaque payload-free refused marker, on
the same channel. Connection/session routing quietly consumes retired or
missing routes; a live mapped session advances its physical Transfer id/window
and wakes its ordinary automatic window update, without LinkRelay, body,
transaction-manager or listener work. Plain, TxnSession, ListenerSession and
TxnSession<ListenerSession> delegate to that single session implementation.
The marker is incoming-only and has no wire encoding or payload exposure.

`ReceiverTransferObserver::transfer` receives one ReceiverTransferProgress
per consumed Transfer after the native semantic decision, outside native locks.
The native parser alone commits PayloadIncomplete, Delivered{info}, Aborted,
Ignored{more}, StateOnly{info,retained,more}, or Failed. A state-only initial
fragment snapshots the exact DeliveryInfo under the native map write; later
fragments carry no new identity. A settlement removing it before that write
produces None/false. All normal, exact-suspension, Close-prefix and stopped receive loops use
this same parser and callback contract; the observer never reclassifies raw
Transfers and no new record or body surface is added. Four private no-await
parser helpers are synchronous, preserving their no-yield ordering.

Regressions are `a_refused_transfer_never_leaves_as_a_normal_frame`,
`refused_transfer_keeps_only_physical_session_progress`,
`a_refused_transfer_on_a_retired_session_is_consumed_quietly`,
`observed_abort_then_delivery_retires_only_the_partial`,
`observed_ignored_resume_consumes_every_fragment_without_a_delivery`,
`observed_state_only_supplies_the_exact_native_identity_once`, and
`state_only_settlement_before_the_native_write_supplies_no_identity`. They
exercise actual transport verdicts, bounded connection forwarding, automatic
session-only Flow, all wrapper kinds, omitted continuation fields and an actual
native settlement at the state-only map-write gate. Boundless W1-D1 supplies
the corresponding router/ingress integration tests; those are not claimed by
these native-only fixtures.

## Associate an allocated Begin channel before dispatch (Boundless W1-D1)

On native targets `Session::builder().begin_with_channel<F>(connection,
allocated)` retains begin's result and bounds and adds a final
`F:FnOnce(u16)+Send` callback. It runs exactly once after successful session
allocation and before any engine/native Begin send. Allocation refusal never
calls it; later dispatch failure retains that one association. Ordinary begin
delegates with a no-op and the wasm/local signatures stay unchanged.
`a_begin_channel_is_associated_before_any_begin_can_escape` gates the real
callback and wire writer, checks the peer's actual channel and following End,
and covers allocation refusal, dispatch failure and the default entry.

### W1-D1 regression evidence

All commands below passed (exit 0) on macOS 27.2 arm64, Rust 1.97.0, at the source
revision above; the following documentation-only commit changes no tested code.
Every compile/test used the machine build-slot wrapper. This fork has no
upstream tracked Cargo.lock; none is added.

```text
cargo test -p fe2o3-amqp --features transaction,acceptor --lib link::sender::tests::
cargo test -p fe2o3-amqp --features transaction,acceptor --lib link::receiver::tests::
cargo test -p fe2o3-amqp --features transaction,acceptor --lib transaction::controller::tests::
cargo test -p fe2o3-amqp --features transaction,acceptor --lib a_begin_channel_is_associated_before_any_begin_can_escape
cargo test -p fe2o3-amqp --features transaction,acceptor --lib a_refused_transfer_never_leaves_as_a_normal_frame
cargo test -p fe2o3-amqp --features transaction,acceptor --lib refused_transfer_keeps_only_physical_session_progress
cargo test -p fe2o3-amqp --features transaction,acceptor --lib a_refused_transfer_on_a_retired_session_is_consumed_quietly
cargo test -p fe2o3-amqp --features transaction,acceptor --test clean_teardown --test link_stop_reason --test detached_link_frames --test engine_first_failure
cargo check -p fe2o3-amqp --no-default-features
cargo clippy -p fe2o3-amqp --features transaction,acceptor --lib --tests -- -D warnings
cargo fmt --all --check
```

The selected native unit filters passed 78 tests; the four existing integration
suites passed 25. This is targeted regression evidence, not a complete crate or
platform acceptance run. The work branch is based on pinned 44afcb79; the landing
owner reconciles its other fork line and fast-forwards boundless. No push or
boundless-branch movement is part of this patch.

### Necessity revision validation

The earlier W1-D1 commands above describe historical dfc0c449 behavior. The
trimmed source is `b64f555c60adf3fb8b5e6e23da762caff6254ed8`, tested on
macOS 27.2 arm64 with Rust 1.97.0 (the identical source tree before commit).
It removes store-identity contracts and duplicate receipt facts; no fork lockfile
was committed. Every following Cargo command ran through `build-slot`, without
`--locked`. Unit test commands share
`cargo test -p fe2o3-amqp --features transaction,acceptor --lib`.

| Command/filter | Exit | Result |
|---|---:|---|
| `link::receiver::tests::` | 0 | 50 passed, including the actual Close-prefix fixture in three end schedules. |
| `incomplete_close_attach_is_not_business_recovery` | 0 | 1 later-added full-validation control passed. |
| `link::sender::tests::` | 0 | 19 passed, with nonempty local unsettled state and null-map close. |
| `transaction::controller::tests::` | 0 | 5 passed, including both command types/closed flags, final admission and outcome-first completion. |
| `a_begin_channel_is_associated_before_any_begin_can_escape` | 0 | 1 passed. |
| `a_refused_transfer_never_leaves_as_a_normal_frame` | 0 | 1 passed. |
| `refused_transfer_keeps_only_physical_session_progress` | 0 | 1 passed. |
| `a_refused_transfer_on_a_retired_session_is_consumed_quietly` | 0 | 1 passed. |
| `cargo test -p fe2o3-amqp --features transaction,acceptor --test clean_teardown --test link_stop_reason --test detached_link_frames --test engine_first_failure` | 0 | 25 passed. |
| `cargo check -p fe2o3-amqp --no-default-features` | 0 | Passed. |
| `cargo clippy -p fe2o3-amqp --features transaction,acceptor --lib --tests -- -D warnings` | 0 | Passed. |
| `cargo fmt --all --check` | 0 | Passed. |

Exploratory checks exposed a test-edit delimiter, stale receipt getter and fixture
type comparisons; these were corrected before the successful regressions above.
The native source and callback contract are unchanged after those runs.


## Incoming session frames crossing normal connection Close (2026-10-28)

AMQP 1.0 Part 2 section 2.4.6 permits incoming frames in CLOSE_SENT until the
peer's Close. The independent Artemis reconnect case reproduced an answering
Attach on channel 0 followed by Flow after local Close; forward_to_session's
Opened-only check returned IllegalState. Temporary state/frame-kind/site
instrumentation identified this check and excluded the other connection
IllegalState producers; no diagnostic code is retained.

Allow CloseSent at the existing incoming session forwarding, Begin-response
binding and End entries. Keep the original session relay and all channel/error
validation. The existing Close control closes and drains outbound admission
before sending Close, so no session output can be emitted afterward. Error-close
Discarding and the peer's actual error-bearing Close retain their behavior.
No public API, new state, queue, retry or lifecycle owner is added.

The deterministic native regression is
connection::engine::incoming_tests::a_local_close_keeps_crossing_session_frames_without_later_output:
receipt of the real Close gates Begin/Attach/Flow/End input; the same relay gets
each frame, outgoing admission is closed, no later frame is written, and normal
versus error-bearing peer Close keep their results. Existing clean_teardown and
engine_first_failure cover clean shutdown and real protocol/transport failures.

Upstream 8fd06bc3 comparison for the paths this patch reaches:

| Path | Upstream behavior | This patch / regression |
| --- | --- | --- |
| Normal local Close with session input crossing its reply | engine.rs::forward_to_session accepts only Opened; connection Begin/End similarly reject CloseSent, so legal input returns IllegalState. | Change to consume legal CloseSent input through the same relay until peer Close, required by transport §2.4.6. The real-Close-gated crossing regression drives Begin/Attach/Flow/End and asserts no later output. |
| Outgoing work after local Close | on_control closes and drains outgoing admission before writing Close. | Kept; the same regression asserts the outgoing writer is closed and no post-Close frame escapes. |
| Peer Close and actual errors | on_incoming_close retains a normal or error-bearing peer result; malformed frames/channel errors retain their error path, and Discarding rejects ordinary session input. | Kept; crossing regression runs normal/error peer Close, engine_first_failure drives real invalid-frame and transport errors. No EOF normalization. |
| Cancellation, timeout and retry | ConnectionHandle::close sends its control then waits on on_close; dropping the wait cannot retract an already sent control. There is no autonomous close retry or built-in close timer here. | Kept: this patch changes no handle wait/bounds/retry path; the engine still owns the issued Close. No new retry, timer or cancellation policy. |

Validation on macOS arm64 / Rust 1.97.0 uses the consumer's tokio-util 0.7.19 in a local,
uncommitted fork lock (fresh resolution selects 0.7.20, whose new Encoder<&[u8]>
implementation makes pre-existing transport calls ambiguous). The fork still
carries no committed Cargo.lock.

Commands (through build-slot):

```sh
cargo update -p tokio-util --precise 0.7.19 --offline
cargo test -p fe2o3-amqp --features transaction,acceptor --lib --test engine_first_failure --test clean_teardown
cargo clippy -p fe2o3-amqp --features transaction,acceptor --lib --tests -- -D warnings
cargo fmt --all --check
```

Results at this source tree (before commit): the two incoming-engine unit tests
pass (exit 0), clean_teardown's four tests and engine_first_failure's five tests
pass (exit 0); strict Clippy and fmt pass (exit 0). The broader library run is
166 passed / 1 failed (exit 101): the existing receiver test
accepted_detach_commits_one_frame_and_one_state_transition still asserts that
Close removes its output handle (link/mod.rs:1400), contradicting the previously
landed receiver Close-prefix preservation at the same file's send_detach entry.
That unrelated assertion is unchanged by this patch; its correction was referred
to the consumer owner. The deterministic real-Artemis reproduction and final
consumer checks are retained by the consuming change. No temporary diagnostic,
local lock or dependency override is part of this commit.


## Receiver Close admission assertion (2026-10-28)

Update the pre-existing accepted_detach_commits_one_frame_and_one_state_transition
assertions to retain the receiver's original output handle 7 after its closing
Detach is admitted and after a rejected duplicate Detach. W1-D1's landed receive-preserving Close requires that binding
until the first native Detach, as send_detach already implements. State, exact
frame, dispatch and nonclosing assertions stay unchanged; no runtime change.
The consumer owner explicitly approved this test correction. On macOS arm64,
Darwin 27.2.0, Rust 1.97.0, the final source tree passes the complete library
(167 tests), the six regression binaries (34 tests), strict Clippy and fmt,
each with exit 0. Commands, run through build-slot on 2026-10-28, using the same
uncommitted local lock described above (the fork has no tracked Cargo.lock):

```sh
cargo test -p fe2o3-amqp --features transaction,acceptor --lib --test clean_teardown --test link_stop_reason --test detached_link_frames --test engine_first_failure --test large_message_split --test relay_remote_detach
cargo clippy -p fe2o3-amqp --features transaction,acceptor --lib --tests -- -D warnings
cargo fmt --all --check
```

These results apply to this assertion-correction commit on top of 66bed4b8;
only this validation note changed after the checks. No runtime change.


## Interrupted resume uses upstream suspension cleanup (2026-10-28)

The Boundless interruption/timeout adaptation returned the endpoint immediately,
while an admitted Attach still had an unanswered peer response. On the real
Artemis reconnect case, the consumer restored its old router binding, then
rejected that late Attach as a duplicate and aborted an unrelated normal Close.
The capture shows local Attach/Close, remote Attach/Flow, consumer refusal, then
remote normal Close and a successful proxy copy; it is not a broker EOF first.

Restore the standard cleanup from upstream 0.18.1 (8fd06bc3,
Sender::resume_with_timeout_inner): the ownership-returning explicit interruption
entries and native timeout entries call existing detach_with_error(None) before
returning the detached endpoint. Its existing LinkState match sends nothing for
Detached (before Attach admission), and sends/completes a nonclosing Detach for
an admitted half-open Attach, consuming the late Attach and Detach answer. Keep
upstream's actual cleanup error on failure. The consumer's existing owned resume
retains its recovering scope until this completion; the caller's wait is separate.
No new signal, state, cell, replay or fallback owner. Apply the same original
sender/receiver timeout class, preserving native unsettled/partial custody.

The deterministic detached_link_frames regression covers interruption before
and after real Attach admission, actual paused-clock timeout expiry, successful
and error-bearing cleanup answers for both roles, and an actual subsequent resume; the existing partial-receiver pre-dispatch
regression retains its original no-frame and partial-body assertions. The
consumer keeps its original successful logical Close and joined-pumps assertion.
Upstream 8fd06bc3 comparison for each changed or retained resume path:

| Path | Upstream behavior | This patch / regression |
| --- | --- | --- |
| Native timeout | All timed sender/receiver resume variants cancel their in-progress exchange, call detach_with_error(None), then return Timeout with detached custody; cleanup failure is returned instead. | Restore that cleanup in every timed variant. The shared regression uses an actual resume_on_session_with_timeout for both roles, pauses Tokio time, holds the late Attach and advances its deadline. |
| Explicit cancellation | Upstream has no interrupt-future entry; dropping its owning resume future drops its endpoint custody. | Keep the existing fork's ownership-returning Interrupted API, but use the same native suspension cleanup before returning custody. The regression signals that exact entry after real Attach admission; no dropping of the native owner is substituted. |
| Interruption before dispatch | Native detached state makes detach_with_error a no-op. | Kept; regression's ready interruption sees no Detach before the next real Attach, and the existing partial receiver fixture keeps its no-frame/body-custody assertions. |
| Half-open cleanup | The helper selects AttachSent but send_detach rejects it; outgoing session Detach also deletes the pending relay before the late Attach can bind. | Correct this upstream defect as P9 below, using the same state admission and close_pending/attachment owner. Regression withholds the Attach reply until it observes nonclosing Detach. |
| Cleanup error | try_as_sender/try_as_recver return actual Detach error rather than substituting Timeout; cleanup never implies commit/replay. | Kept; the shared regression answers cleanup with a nonclosing error Detach and asserts the original typed error condition for both interruption and timer expiry, then explicitly resumes the retained endpoint. |
| Retry | Detached custody is returned to the caller; another resume is explicit. | Kept; regression explicitly invokes a subsequent resume and the peer sees its actual Attach/credit. No automatic replay, retry or Discharge. |

The fixture enables Tokio test-util only for dev dependencies to advance the real
native timeout while the actual late-answer gate is held; no product clock or
new cancellation signal is introduced.

The class includes both explicit interruption entries and every timed resume
entry. Restore the same cleanup in sender resume_with_timeout_inner and
resume_incoming_attach_with_timeout_inner (including their public on-session
wrappers), and receiver resume_with_timeout_inner,
resume_incoming_attach_with_timeout and
resume_incoming_attach_on_session_with_timeout. The shared after-dispatch
regression covers both roles; all wrappers use the same existing native state
and attachment path rather than independent recovery behavior.

The first deterministic run exposed the upstream 8fd06bc3 half-open defect:
shared_inner::detach_with_error selects AttachSent, but Link::send_detach
accepted only Attached and returned IllegalState. The session also removed its
pending name/relay as soon as Detach was sent, before the late Attach could bind.
The consumer owner approved correcting that native path (P9 A).

Allow nonclosing Detach for AttachSent/IncompleteAttachSent, with the existing
state transition committed only after queue admission. Session::on_outgoing_detach
retains a still-pending relay/name and original attachment allocation, using the
same close_pending owner. The current-output checkpoint excludes close_pending,
so Transfers and link Flow remain suppressed after Detach, even while its native
binding is retained for answers. The original incoming Attach binds that relay;
the answering Detach removes close_pending and releases the exact allocation.
Attached detach, ordinary duplicate/name checks, actual errors and existing
attachment-id checks keep their behavior. No new signal, cell, history or owner.

AMQP 1.0 Part 2 sections 2.6.4 and 2.6.5 describe Attach then Detach; a local
endpoint need not wait for an answering Attach before detaching. Mature source
checks match this: azure-amqp/src/StateTransition.cs sendClose permits OpenSent
to ClosePipe (lines 26–32), and rhea/lib/link.js::_process sends its Detach after
its Attach without waiting for the peer (lines 118–132). The regression holds
that late Attach until it sees the nonclosing Detach and then verifies an actual
subsequent resume, for both roles; its next expected Attach also catches any
post-Detach Transfer/Flow. Before-dispatch interruption sends nothing.

Final source validation at 24977d12 on macOS arm64, Darwin 27.2.0, Rust 1.97.0,
using the consumer-compatible uncommitted local lock (tokio-util 0.7.19):
all 167 library tests and 35 regressions pass, exit 0; strict Clippy and fmt pass,
exit 0. All commands run through build-slot on 2026-10-28:

```sh
cargo test -p fe2o3-amqp --features transaction,acceptor --no-fail-fast --lib --test clean_teardown --test link_stop_reason --test detached_link_frames --test engine_first_failure --test large_message_split --test relay_remote_detach
cargo clippy -p fe2o3-amqp --features transaction,acceptor --lib --tests -- -D warnings
cargo fmt --all --check
```

Only this validation note changed after those checks. The consuming change
records its independent Artemis and Boundless checks; no local lock, temporary
diagnostic or dependency override is committed.
## W4e: retain allocation answers after cancellation

At 44afcb79, connection/engine.rs:418-422 and session/engine.rs:337-346
turn an abandoned allocation reply into an engine error. The local-link
allocator at :323-333 releases an allocation only if send itself fails; a
successful but unread reply can still lose its slot. Both races belong to
one native reply checkpoint, `completion::{Sender,Receiver}`. Its disposition
is installed before handoff, and receiver Drop closes then takes, following
Tokio oneshot's close/try_recv ownership ordering (Tokio 1.52.3,
src/sync/oneshot.rs:947,1171,1246). No callback runs under a channel lock.

`CompletionSink<T>::scope(operation)` supplies a per-operation sink before
polling. Allocation sinks receive `Result<u16, AllocSessionError>` or
`Result<u32, AllocLinkError>`. Engines also retire untaken local allocations;
retirement is bounded by the allocated population, wakes the engine, and is
drained before another allocation can reuse a slot. Incoming-link retirement
also removes that attachment's input relay. Cancellation never ends siblings.

Regressions: `late_session_allocation_retires_without_ending_connection`,
`abandoned_allocation_answers_reach_sink_and_release_both_roles`, and
`completion::tests` cover both handoff orders, normal consumption, and concurrent
send/drop. Restoring original native oneshot disposition behavior (no handoff
on failed send or unread receiver Drop) fails the allocation tests (exit 101).
With the patch the all-feature library suite passes (150 tests, exit 0), on
macOS arm64 / Rust 1.98.1. The fork's existing untracked Cargo.lock is used;
a fresh resolution upgrades tokio-util and fails untouched transport sink
type inference, outside this patch.

## W4e: settle every delivery through its captured disposition

At 44afcb79, link/delivery.rs:315-352 returns failed oneshot sends to
callers that discard them; Tokio also drops a successfully sent but unread
settlement. `send_new_payload` now captures the scoped
`CompletionSink<Result<Option<DeliveryState>, LinkStateError>>` in the native
channel. `UnsettledMessage::{settle,settle_with_state,fail}` all use the same
completion checkpoint. The receiver stays wrapped through `Settlement`,
`DeliveryFut`, and transaction controller Declare/Discharge, including tracked
control sends. Recovery keeps the original unsettled message and its sink.

`native_settlement_hands_every_untaken_outcome_to_its_sink` sends through the
actual sender admission path and exercises all three completion methods in
both handoff orders, retaining the Accepted outcome or the actual failure.
With the preceding raw oneshot handoff restored in `send_new_payload`, it
fails (exit 101); the all-feature library suite passes with the patch
(151 tests, exit 0), on macOS arm64 / Rust 1.98.1.

## W4e: keep resource-side transaction reply ownership

At 44afcb79, session/engine.rs:382-408 converts a failed transaction-control
reply send into UnattachedHandle, ending an otherwise usable session. The
AllocateTransactionId, CommitTransaction and RollbackTransaction reply channels
now use the same per-operation completion disposition as allocation and
settlement; abandoning a reply is no engine error. The sink receives the native
`Result<TransactionId, AllocTxnIdError>` or `Result<Accepted, TransactionError>`.
No Discharge or rollback is introduced by cancellation.

`late_transaction_control_replies_do_not_end_the_engine` exercises the actual
transaction session's three controls in both handoff orders, checks exactly one
native answer and verifies the session remains mapped. Restoring original
failed-send ownership at the shared native checkpoint makes it fail (exit 101).
The regression passes with the patch (exit 0), on macOS arm64 / Rust 1.98.1.

## W4e: preserve engine answers at the last native handoff

At 44afcb79, util/engine_outcome.rs:16-32 ignores a failed terminal-result
send, and its private receiver/buffer can drop an unread answer. The same
completion channel now owns engine dispositions, including the result retained
by `probe` while `stop_and_join` awaits the task. A scoped sink installed around
an engine wait is captured before its await; cancelling that waiter closes then
takes, and a result completed later goes to the sink. Without a supplied sink,
the existing retained stop/join behavior stays. The atomic natural-end versus
owner-stop arbitration and stop reasons are unchanged (W5-D1 is separate).

The two `engine_outcome::completion_tests` cover sender-first, receiver-first,
probed-buffer drop, and cancelling a borrowed engine wait before its result.
Restoring the preceding raw oneshot handoff fails both regressions (exit 101).
All 154 all-feature library tests pass, including the existing local-stop race,
panic and repeated-stop tests (exit 0), on macOS arm64 / Rust 1.98.1.
Production/integration compilation also keeps the session engine's protocol-type
imports unconditional after retiring its test-only oneshot import; otherwise
unit-test-only compilation masks the missing production imports.

## W4e: retire only an abandoned allocation's input routing

The allocation disposition must not remove routing needed by an ordinary
closing or non-closing Detach exchange. The first W4e implementation removed
input relays in `Session::deallocate_link`, which also runs when a live sender
sends Detach. Its peer's reply then lost its route, and retained deliveries
failed to resume with `DetachError(IllegalState)`.

`Session::retire_link_allocation` is the one cleanup point for an allocation
reply nobody took. The engine's private retirement queue calls it; the listener
and transaction session wrappers delegate it. Ordinary deallocation retains
its existing detach routing. Attachment identity still prevents an old
retirement from removing a later allocation that reused its handle.

Regression evidence: the existing `detached_link_frames` cases
`a_paused_delivery_never_written_is_sent_again_whole_once`,
`a_delivery_paused_in_a_later_fragment_is_resumed_whole_once`, and
`a_delivery_the_peer_holds_is_resumed_under_its_tag_without_new_credit` all fail
on the first W4e implementation. They exercise the native effect; the allocation
checkpoint tests also require an abandoned incoming allocation's relay to be
removed. On 198d6552 all six `detached_link_frames` tests pass (exit 0),
on macOS arm64 / Rust 1.98.1, including those three regressions.

## W17c: configured bounds at the unsettled owner

At 44afcb79, link/unsettled_store.rs:54-57,110-128,155-158 exposes an
unrestricted mutable map and unbounded sender registration/state replacement;
link/mod.rs:803-826 inserts receiver entries directly. Replenished protocol
windows therefore permit retained unsettled state to grow without an aggregate
bound. Recovery and receiver receipt/settlement updates also change retained
state through that mutable map.

`Store::new(value, limits)` accepts optional `UnsettledLimits::new(max_entries,
max_bytes)`. `Sender`/`Receiver`/controller builders, `LinkAcceptor` and
`ControlLinkAcceptor` builders expose `unsettled_limits(limits)`. No deployment
cap values are chosen: omitted limits preserve unconfigured upstream behavior.
The same Store and limits remain owned across detach/resume and reattach.

`Mutation::check` is the single capacity checkpoint for constructor retention,
registration and size-changing updates. Raw mutation is private, with no
DerefMut or mutable-map guard outside Store. Receiver updates, receipt metadata,
settlement batches and recovery reconciliation use its bounded methods. Removal
returns room. Admission checks run before credit/queue waits as well as under
the actual handoff lock, so known exhaustion cannot wait for unrelated credit.
This follows the pre-push semantic-owner pattern in russh 7596d8a,
russh/src/channels/inbox.rs:45-74, rather than allocator callbacks.

The byte limit counts tags, sender payloads, encoded local/remote delivery states
and retained delivery-info tags; the separate entry limit bounds structural
population. It is a retained-content bound, not an allocator-memory measurement.
Checked arithmetic refuses unrepresentable sizes. Exhaustion permanently closes
the unsettled owner with `UnsettledCapacityExceeded`, fails pending sender
outcomes outside the lock through W4e's completion disposition, and is retained
through recovery. Receiver-relay exhaustion ends its session through the normal
ResourceLimitExceeded End exchange, retaining the typed cause if cleanup fails.

Regression evidence: Store count/byte refusal, sender state growth, receiver
local/remote growth, replacement/removal room, and permanently ended recovery;
`unsettled_sender_caps_precede_credit_and_dispatch_and_fail_pending_outcomes`,
`unsettled_exhaustion_is_known_even_without_peer_credit`,
`unsettled_receiver_relay_caps_count_and_bytes_before_forwarding`, and
`unsettled_recovery_reconciliation_refuses_state_growth` cover real callers.
Restoring the preceding unbounded capacity policy makes the regression selection
fail (exit 101); restoring the checkpoint passes all 13 selected library tests
(exit 0), on macOS arm64 / Rust 1.98.1. `tests/unsettled_limits.rs` exercises both
configured roles across a replenished one-frame session window and link credit.

The new typed native error variants require corresponding consumer match arms
when integrating this pin. The fork does not classify them by diagnostic text
or manufacture a peer close. Boundless's W17c integration owns that mapping and
selects measured constructor values later.

## W17c: keep the portable cap regression's cleanup separate

The first final portable run on 198d6552 passed the cap assertions but failed
at tests/unsettled_limits.rs:91: cleanup aborted the session readers before
stopping their connections, then required a clean client connection result.
A queued frame forwarded to an aborted reader produces the baseline
connection/error.rs:183-189 `NotFound("Session is not found")`; either peer can
also observe EOF during local transport shutdown. Neither is a cap assertion.

The same regression now stops both connections concurrently before aborting
and joining their session readers. Its configured sender/receiver, replenished
window, typed exhaustion, pending outcome and connection-alive assertions stay
unchanged. The failing run is exit 101; five focused reruns of that same test
pass after the cleanup correction (one test each, exit 0), on macOS arm64 /
Rust 1.98.1. No production code or public API changes.

## W17c review F1: interrupt sends when their unsettled owner ends

On d494f0b6, sender_link.rs:264-275 waits for link credit and queue room
without observing a capacity failure published after admission. The continuation
and resuming handoffs at :71 and :340 have the same uninterruptible queue wait.
Rule 11 requires terminal exhaustion to return its retained typed cause without
waiting for unrelated peer progress.

`Store::until_terminated` is the shared wait checkpoint: subscribe to the
existing coalescing revision before checking the retained cause, race the native
operation against changes, and recheck before returning its result. It follows
Tokio 1.53.3 sync/watch.rs:983-1001's notification-before-version-check pattern.
Sender `deliver` and `send_resuming_transfer` guard their entire native operation;
only their private inner implementations can reach the credit/queue waits.
Admission and registration still check under the Store mutation lock. No extra
error ledger, peer progress requirement, or completion owner is introduced.

`review_cap_exhaustion_interrupts_inflight_credit_and_room_waits` fails on the
preceding branch (both waits remain Pending, exit 101), and passes here (exit 0).
`cap_termination_interrupts_fragment_and_resuming_handoffs` covers continuation
and resuming transfers. Both tests pass (2 tests, exit 0).
`terminal_wait_wakes_and_retains_capacity_metadata` checks the single checkpoint's
actual wake and exact retained metadata (1 test, exit 0). macOS arm64 / Rust 1.98.1.

## W17c review F2: retain capacity termination for an already waiting receive

On d494f0b6 receiver.rs:1167 waits only on its input. Exhaustion during that
wait is later reported as generic SessionStopped(ResourceLimitExceeded), while
a receive begun afterwards reports the Store's typed limits and usage.
`recv_inner` now runs through `Store::until_terminated`; its private implementation
contains every receive, partial-transfer, automatic disposition/flow and self-close
await it can start. The closed-input fallback also checks the retained Store
cause before deriving a generic session stop. W5-D1's stop-reason shape is untouched.

`review_cap_exhaustion_preserves_typed_cause_for_already_waiting_receiver` fails
before (exit 101), and passes after (exit 0). It now compares the exact retained
metadata for waiting and subsequent receives. `cap_termination_wakes_receiver_before_input_closes`
also requires the same result while the input sender remains alive. Both receiver
tests and the two sender wait regressions pass (4 tests, exit 0), on macOS arm64 /
Rust 1.98.1. All paths use the same Store checkpoint, with no copied terminal ledger.

## W17c review F3: reject recovery of a terminal unsettled owner

On d494f0b6 sender_link.rs:770-781 treats a cap-cleared Store and empty peer
map as Complete. sender.rs:1094 then reports successful recovery. Sender and
receiver allocation, Attach admission and Attach-reply waits could also cross a
new terminal transition and lose its cause.

Both role Attach handlers check the retained Store before reconciliation.
Both role business send/exchange Attach methods and resume operations use
`Store::until_terminated`. Already-admitted teardown uses the cap-safe shared
control passage described in the W17c2 section below. The private endpoint
trait ties its unsettled associated type to that Store, so generic recovery
cannot bypass the same checkpoint. Recovery on another session checks termination
before altering the endpoint; sender resend also checks before an empty-map return.
No allocator request or Attach is emitted for an already exhausted owner.

`review_cap_exhaustion_cannot_resume_sender_as_complete` fails before with
Ok(Complete) (exit 101), and passes after (exit 0). For both roles,
`cap_recovery_rejects_before_allocation_and_interrupts_allocation` covers resume
and reattach, and `cap_recovery_interrupts_attach_queue_and_reply_waits` covers
both queue admission and waiting for Attach. All nine cap wait/recovery tests
pass (exit 0), on macOS arm64 / Rust 1.98.1. The recovery-wait regressions compare
complete retained limits and proposed usage; the empty-map probe requires the
retained typed termination.

## W17c review same class: control, relay and settlement waits retain termination

At d494f0b6 the raw waits are receiver_link.rs:31-57,230-282;
receiver.rs:742-805,920-921,1772-1783; sender_link.rs:614-724;
shared_inner.rs:334-371; and link/mod.rs:633-640,665-668,802-811,856-879.

The same failure occurs beyond sends/recv/recovery: receiver flow/drain,
ordinary and batched dispositions on both roles, ReceiverDisposer's disposition
and automatic credit refresh can wait on unrelated queue room or input. An already-draining receiver also returned Ok after its
Store ended. Transfer and transaction-acquisition forwards could remain blocked after the
native reader observed exhaustion and stopped polling.
The receiver settlement view woke with Ok on a cap-purged map, exposing only
absence, which could be mistaken for peer settlement.

These paths now use the same `Store::until_terminated` checkpoint. The drain
short circuit checks the retained cause. Transfer forwarding propagates the
typed error through both ordinary and acceptor sessions; ordinary dropped-reader
errors remain UnattachedHandle. Attach/Detach forwarding and shared cleanup waits
retain the admitted exchange's actual native result as described below. Existing sender pending outcomes already fail through Store
outside its lock, preserving W4e's exactly-once handoff.

Public `ReceiverSettlementView::changed` now returns `Result<(), LinkStateError>`
with the retained capacity error. Its new `check()` reports that same cause for
synchronous binding queries. After observing absence, consumers must check this
status before classifying a purge as peer settlement. No new terminal ledger or
error variant is added; the existing LinkStateError mapper can carry the cause.

`cap_control_*` regressions reproduce eight failures before this patch (one
pending DeliveryFut check already passes, exit 101). All nine pass after (exit 0).
They compare exact typed limits/usage across receiver flow, drain, single/batched
and cloned-disposer settlement, both automatic-flow paths, sender dispositions,
receiver settlement view in both orders and relay Transfer forwarding.
The historical early Detach/Attach refusal assertions are superseded by W17c2's
shared actual-completion fixture below. The native pending DeliveryFut regression
continues to pass. Tests use explicit Pending/Ready checkpoints, not elapsed time.
macOS arm64 / Rust 1.98.1. The full library and portable reruns are recorded in the
final verification evidence. Boundless semantic integration remains with W4e/W17c.

## W17c: keep terminal guards from growing the caller's recovery stack

The first broad portable rerun on 6ad35fee failed with a native stack overflow
in `a_paused_delivery_never_written_is_sent_again_whole_once`, before a test
assertion. `Store::until_terminated` kept each guarded operation inline in its
async state (unsettled_store.rs:128-150); nesting recovery, Attach, detach and
control guards multiplied the caller's future layout and move stack.

The same checkpoint now pins the operation on the heap synchronously before
constructing its small guard future. This follows Tokio 1.53.3 task/spawn.rs:
175-185's boxing of large futures, with no new task or semantic limit. Wake,
retained-cause and native completion ownership are unchanged. It adds one fixed
future allocation per guard, not a growing state queue or a deployment cap.

The existing portable `detached_link_frames` target fails before (exit 101,
SIGABRT from stack overflow) and passes all six tests after (exit 0), using the
same test stack configuration. No test assertion or stack setting was changed.
macOS arm64 / Rust 1.98.1. This is a regression of the new terminal guard;
W4e's commits and detach routing remain unchanged.


## W17c2: preserve owned crossed teardown after Store exhaustion

This work branch starts at ea211cd459f12c61f942e1c01ca01e26f64b39cf.
The required W4e completion commits were ported first; their original retirement
queue/channel remains the one owner of abandoned allocation and engine answers.
ControlTracker's receipt-before-notification, parser-step receiver Close and the
pin's late half-open Attach/name/relay custody are retained. Historical commands
above belong to their named revisions, not this port. Checkpoint 14d3bbf1 records
the reconciled Store/reporter/preflight tree before the crossed amendment.

Upstream 8fd06bc36f2e3819ba4f01256b6688a2ce0d627a
`sender.rs::resume_with_timeout_inner` completes nonclosing suspension after its
actual timeout. `shared_inner.rs::reattach_then_close` then uses ordinary Attach
reconciliation for a crossed closing answer, clearing the local map before the
new answer. The pin extends that owned cleanup to interruption and half-open
Attach. Keep that cleanup ownership, original late-answer binding and error
precedence. The deliberate divergence is confined to the *subsequent teardown
reattachment*: use the existing CloseOnly Attach, with null local map, common
validation and incomplete-map refusal, then consume the actual closing Detach
answer. A complete nonempty peer map must not yield Resume/IllegalState or supply
Accepted/Rejected business knowledge during teardown.

Mature source comparison: Qpid Proton
3f263156a9a4ea3fd3c2eb78dbd796314958fd0b `c/src/core/transport.c::pn_do_detach`
and `python/proton/_handlers.py::on_link_remote_close`; rhea
d7474da72add1470be23a974ff670cff3d995e7f `lib/link.js::{on_detach,_process}`.
Their teardown does no unsettled reconciliation. They do not provide fe2o3's
automatic crossed handshake; that handshake remains necessary here. Teardown
needs no business reconciliation, and Store-cap cancellation would otherwise
strand already admitted cleanup.

The private `reattach_for_close` is the common generic/exact teardown door.
Shared allocation, Attach/Detach forwarding, error cleanup and reply waits do
not race the Store latch. Actual Session/Connection failure still terminates
through the ordinary native result. Business admission/resume/receive/flow and
settlement keep the same irreversible Store check; clearing never reopens it.
No callback/token/registry, public resume-signature change, sibling-preservation
policy or second outcome cell was added. Session's construction-bound capacity
reporter runs synchronously before its normal resource-limit End, and detached
read-only checks expose the same immutable Store for adapter admission.

Regression fixture: `tests/detached_link_frames.rs::
interrupted_resume_consumes_the_late_attach_and_returns_resumable_custody` calls
one shared scripted peer in `tests/support/interrupted_resume.rs`. Its private
Store wrapper `link::cleanup_capacity_tests::
admitted_resume_cleanup_outlives_interrupt_and_cap` uses real checked Store
registration/insertion, with no public test hook. Both roles, real interrupt and
paused-clock timeout cover held original Attach/first Detach before allocation,
CloseOnly Attach and final closing Detach answers. They assert retained custody
through cap, actual completion/Session failure, irreversible cap after clear,
no business replay or Accepted/Rejected inferred from teardown Attach, and keep
a previously confirmed delivery's real result. The noncrossed later explicit
resume, pre-dispatch and actual peer cleanup-error arms remain. Both old endpoint
Detach-cap early-return tests and obsolete relay early-drop assertions are gone;
no ordering-only replacements were added.

Validated source tip: 2dd8f15a. On macOS Darwin 27.2.0 arm64, the following
commands ran through build-slot. Tests and strict Clippy use Rust 1.97.0;
the formatting row states its separate toolchain.

| cargo arguments | Result |
| --- | --- |
| test -p fe2o3-amqp --features transaction,acceptor --lib cap | 22 passed |
| test -p fe2o3-amqp --features transaction,acceptor --lib unsettled_store | 5 passed |
| test -p fe2o3-amqp --features transaction,acceptor --lib completion | 5 passed |
| test -p fe2o3-amqp --features transaction,acceptor --lib link::sender::tests:: | 29 passed |
| test -p fe2o3-amqp --features transaction,acceptor --lib link::receiver::tests:: | 58 passed |
| test -p fe2o3-amqp --features transaction,acceptor --test detached_link_frames | 7 passed |
| test -p fe2o3-amqp --features transaction,acceptor --test unsettled_limits | 1 passed |
| clippy -p fe2o3-amqp --features transaction,acceptor --lib --tests -- -D warnings | passed |
| fmt --all --check | exit 0 on default Rust 1.98.1; pinned 1.97.0 formatting recheck pending |

The A6 regression fails (exit 101, "Store exhaustion cancelled admitted cleanup")
when the old `recv_remote_detach` Store race is restored temporarily; restoring
the reviewed implementation passes. The initial expanded fixture also exposed
its ordinary cleanup helper's expected RemoteEnded assertion on the deliberately
ended-Session arm (exit 101); the arm now asserts the actual RemoteEnded result.
The cap-filter rerun initially caught the two historical relay early-drop
assertions (exit 101); their superseded assertions were removed, with the real
Transfer refusal still tested. The first Clippy check used the fork's default
Rust 1.98.1 and reported pre-existing inline-frame result-size diagnostics;
consumer-pinned 1.97.0 strict Clippy passes, without new result-size exceptions.

Local untracked Cargo.lock selects tokio-util 0.7.19, matching the consumer lock;
no transport/manifest workaround is added. Earlier exploratory compiles found
port fixture mismatches and latest tokio-util codec ambiguity; the pinned focused
results above supersede those failures. The consuming implementation ledger
retains the exact command/exit logs. This documentation-only evidence commit
follows the tested source tip; no push or boundless-branch movement occurred.

Consumer implementation stopped on its required disk preflight at 29 GiB free
on 2026-10-29. The pinned formatter recheck is not claimed complete. No additional
compile/check or cleanup deletion was attempted after that stop.

Pinned-toolchain formatting recheck (2026-10-29, macOS arm64):
`RUSTUP_TOOLCHAIN=1.97.0 build-slot cargo fmt --all --check` exited 0
on source tip `2dd8f15a006691c57955eda11afe3aa1974facdd`. No source changed for this recheck.

## W17c review: preserve peer knowledge across Store exhaustion

Upstream 8fd06bc36f2e3819ba4f01256b6688a2ce0d627a
`link/receiver.rs::on_incoming_transfer` classifies unknown resumed tags from
its ordinary unsettled map; it has no semantic-cap purge. The current fork
adds irreversible purge, so a missing tag is no longer sufficient evidence of
peer completion. The existing Store latch is now checked after recovery-map
queries and before publishing Ignored or StateOnly. Actual parsed peer aborts
and settlements keep their existing meaning. No joint native/consumer lock or
second status owner is introduced.

Upstream `link/sender_link.rs::send_payload_with_transfer` returns its settlement
receiver after successful transfer handoff; it has no Store-cap cancellation.
Keep this result ownership, and this fork's necessary registration-before-send
repair. The W17c whole-delivery cap guard could replace successful handoff with
a later capacity error and discard an already ready Accepted. Guard only pending
credit/queue reservation, before handing the frame to the queue. Keep cap wakeups,
fragment cancellation/withdrawal and detach precedence; after final handoff the
original sender gets its settlement receiver even if another delivery exhausts
the Store. Unresolved outcomes still receive the same retained typed error.

Deterministic regressions use real resumed-Transfer parsing and the native relay
state-update/settlement paths. `cap_purge_cannot_commit_absence_as_peer_recovery`
covers both classification and StateOnly retention queries; the old source emits
Ignored instead of Failed (exit 101). `completed_handoff_keeps_accepted_before_another_delivery_caps`
holds the final handoff, confirms A, then grows B's actual Received state; the
old guard drops A's ready outcome with UnsettledCapacityExceeded (exit 101).
The existing pending-credit, queue and fragment cap tests continue to pass.
The optional `test-util` feature exposes only one-shot, lock-free callback
checkpoints at these exact boundaries for consumer effect regressions; normal
builds contain neither the callbacks nor their storage.

Validation on macOS arm64, Rust 1.97.0, from c129a992 plus this change, all via
build-slot (exit 0):

- `cargo test -p fe2o3-amqp --lib --features transaction,acceptor -- cap`: 24 passed.
- `cargo test -p fe2o3-amqp --lib --features transaction,acceptor,test-util -- link::receiver::tests::`: 59 passed.
- `cargo test -p fe2o3-amqp --lib --features transaction,acceptor,test-util -- link::sender::tests::`: 30 passed.
- `cargo clippy -p fe2o3-amqp --lib --tests --features transaction,acceptor,test-util -- -D warnings`.
- `cargo fmt --all --check`.
