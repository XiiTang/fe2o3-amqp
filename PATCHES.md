# Runtime integration additions

Baseline: upstream fe2o3-amqp history retained on `codex/runtime-engine`. Earlier commits on this branch implement bounded duplex transport, session and link ownership, native settlement and delivery resumption, and explicit transaction lifetime.

The physical suspension change adds `Sender::into_detached` and tightens `Receiver::into_detached`. Only a detached endpoint or an endpoint whose parent session stopped can be retained. Locally closed endpoints and a sender with a queued peer-closing Detach cannot be resumed. Suspension performs no network cleanup or inferred transaction operation.

Validation: `cargo test -p fe2o3-amqp --lib --features transaction,acceptor` passes 74 tests, including active/stopped/closed/queued-close sender suspension and receiver partial-delivery retention. Runtime reconnect, frozen authentication reuse, independent broker recovery, and cancellation remain separate integration acceptance obligations.
