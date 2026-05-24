## Context

Sequential steps in `Pipeline.executeShared` run on the calling thread with no time bound. The engine has no mechanism to interrupt a step that blocks (e.g., an HTTP call waiting on a slow downstream service). `RetryPolicy` is the only per-step configurable policy on `StepDescriptor` today; the design pattern it established — an immutable value type, a fluent `withX(...)` method on the descriptor, engine enforcement invisible to step authors — is the right template to follow.

The engine already uses an `ExecutorService` (defaulting to `ForkJoinPool.commonPool()`) for parallel branches. `StepDescriptor` is a Java record, so adding a new policy component follows the same form as `retryPolicy`.

## Goals / Non-Goals

**Goals:**
- Add a `TimeoutPolicy` value type a caller can attach to any `StepDescriptor` via `withTimeout(...)`.
- The engine enforces the deadline around each `step.execute(...)` call (including retried attempts).
- A timed-out step surfaces as a `Failure` whose `cause()` is a `StepTimeoutException`; the retry loop treats this like any other failure.
- Step authors write zero timeout-related code.

**Non-Goals:**
- Pipeline-level timeouts (only per-step).
- Forcibly killing threads (Java has no safe API for this; the engine signals via `Thread.interrupt()`).
- Timeout for parallel branches (each branch already runs on its own executor thread; the calling thread waits with `future.get()` — a timeout there is a future concern).

## Decisions

### Decision: Implement timeout via `Future.get(timeout)` on the existing executor

**Options considered:**

A. **Interrupt-based**: Schedule a `Thread.interrupt()` on the calling thread via a `ScheduledExecutorService` timer; cancel the timer if the step finishes in time.  
   _Problem_: Requires a second thread pool and careful timer cancellation; the interrupt fires on the calling thread so it can interfere with surrounding pipeline logic if the step swallows `InterruptedException`.

B. **`Future.get(timeout)` on the existing `ExecutorService`** (chosen): Submit the timed step to `executor`, call `future.get(timeoutMs, MILLISECONDS)`. On timeout, call `future.cancel(true)` to signal the worker thread via interrupt, then throw `StepTimeoutException`.  
   _Why_: Reuses the existing executor (already the backbone of parallel execution), returns control to the engine cleanly after the deadline, and keeps the timeout logic entirely in `executeItemWithRetry` alongside the existing retry loop. No new thread pool needed.

**Caveat**: Steps with a `TimeoutPolicy` are submitted to the executor rather than run inline. If the executor pool is exhausted (e.g., many parallel branches in flight), a timed step may queue briefly before starting, consuming wall-clock time against its deadline. In practice, `ForkJoinPool.commonPool()` is unbounded in pending-task capacity; users who size their own executor should account for timeout steps.

### Decision: `TimeoutPolicy` mirrors `RetryPolicy` — immutable value type with factory methods

- `TimeoutPolicy.none()` — no timeout (default; `timeoutMs() == 0`).
- `TimeoutPolicy.ofMillis(long ms)` — explicit millisecond deadline; `ms >= 1` required.
- `TimeoutPolicy.of(long duration, TimeUnit unit)` — convenience for other units.

Placing it in `io.flowpipe.api` alongside `RetryPolicy` makes the public surface symmetric and consistent.

### Decision: `StepTimeoutException` is a new checked-to-unchecked boundary exception in `io.flowpipe.api`

The engine catches `TimeoutException` from `future.get(...)` and wraps it in `StepTimeoutException(stepId, timeoutMs)`. Callers who inspect `Failure.cause()` get a typed, meaningful exception rather than a raw JDK `TimeoutException`.

### Decision: A timeout counts as a failure for the retry loop

Each `execute` attempt inside `executeItemWithRetry` gets its own independent deadline. If a step times out on attempt 1 and has `RetryPolicy.fixed(3, 0)`, the engine will retry twice more, each with a fresh deadline. This is the least-surprising behaviour: a transient hang is treated like a transient exception.

### Decision: `StepDescriptor` record gets a `timeoutPolicy` component

`StepDescriptor` is already a record. Adding `TimeoutPolicy timeoutPolicy` as a new component (with `TimeoutPolicy.none()` as the default in `Builder`) is source-compatible for all callers using `Builder` or `withRetry(...)`/`withTimeout(...)`. Callers constructing the canonical constructor directly (unusual and internal) need updating.

## Risks / Trade-offs

- **Thread cannot be forcibly stopped**: `future.cancel(true)` sends an interrupt, but a step that ignores interruption (e.g., a tight CPU loop or an uninterruptible native call) will continue running on the executor thread after the timeout fires. The engine moves on correctly, but the thread may linger until it naturally completes. This is a fundamental Java constraint, not a FlowPipe deficiency — document it.
- **Executor pool sizing**: Sequential steps with `TimeoutPolicy` now consume an executor thread for the step's duration (up to the deadline). Under load, users with tight thread pools may see queueing. Mitigation: document the recommendation to size the executor to account for timeout-wrapped steps, or use virtual threads (Java 21+) in a future update.
- **`StepDescriptor` record expansion**: Adding a component is a binary-compatible change for users on the public API (builder pattern), but any tests using the canonical constructor directly will fail to compile. These are internal and easy to fix.

## Migration Plan

No external migration needed; this is an additive API change. Internal steps:
1. Add `TimeoutPolicy` value type and `StepTimeoutException` to `io.flowpipe.api`.
2. Add `timeoutPolicy` component to `StepDescriptor` (update record, builder, `withTimeout` method).
3. Update `executeItemWithRetry` in `Pipeline` to wrap each attempt in `executor.submit(...).get(timeout)` when `timeoutPolicy().timeoutMs() > 0`.
4. Add unit tests in `flowpipe-core` for timeout enforcement, retry interaction, and `TimeoutPolicy` factory validation.
5. No changes to the `flowpipe-test` module's `Steps` utility are required, though a `Steps.sleeping(id, millis)` convenience helper is useful for timeout tests.

## Open Questions

- Should `TimeoutPolicy` accept `Duration` (java.time) rather than raw `long + TimeUnit`? `RetryPolicy` uses raw `long` milliseconds for simplicity; consistency argues for the same here.
- Should a timed-out step's `TraceEntry` record the actual elapsed time (up to the deadline) or the configured deadline? Recording actual elapsed time is more useful for observability.
