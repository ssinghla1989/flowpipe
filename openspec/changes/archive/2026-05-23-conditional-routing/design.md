## Context

FlowPipe pipelines today execute a fixed, linear sequence of nodes (sequential steps and parallel blocks). There is no mechanism to route execution down different paths at runtime based on data flowing through the pipeline or values in shared state. Real-world API flows frequently need this — skip an enrichment step when the upstream lookup returned empty, route to a refund path vs. a fulfillment path based on order status, etc. Without branching, that decision logic must live in the calling code, making the pipeline an incomplete model of the actual flow.

The existing `PipelineBuilder` already follows a fluent, type-tracking pattern. The engine nodes are a sealed hierarchy (`StepNode`, `ParallelNode`). Branching maps cleanly onto a third sealed variant, `BranchNode`, executed by the same engine loop.

## Goals / Non-Goals

**Goals:**
- `PipelineBuilder.branch(branchId, predicate, ifTrue, ifFalse)` — adds a conditional node that routes execution to one of two typed sub-pipelines based on a runtime predicate.
- Build-time type safety: both arms must have identical input and output types, checked at `build()` (runtime class token) and at compile time (generics).
- Full observability: the branch decision (which arm was taken) is recorded in the `ExecutionTrace`; non-taken steps appear as `SKIPPED` entries in the trace and trigger `StepOutcome.SKIPPED` on the `MetricsRecorder`.
- Predicate failures surface as `Failure` with `failedStepId` equal to the branch id.
- Nested branches are supported implicitly (each arm is a `Pipeline`, which may itself contain a branch node).

**Non-Goals:**
- Multi-way switch / more than two arms (can be composed with nested `branch()` calls).
- Dynamic arm selection (predicate returns a string key that maps to an arm) — not needed and complicates the type model.
- Modifying shared `State` inside the predicate — predicates receive a read-only view; state writes belong in steps.
- Any changes to the parallel execution path or `parallelN`.

## Decisions

### 1. Predicate signature: `BiPredicate<O, StepContext>`

`StepContext` currently exposes only `State` (mutable) and `RequestContext` (immutable). It does not expose the flowing value. A `Predicate<StepContext>` therefore cannot access the data flowing through the pipeline at the branch point.

The predicate is typed as `BiPredicate<O, StepContext>` — the flowing value and the context are passed as separate arguments. Standard `java.util.function.BiPredicate` is used; no new functional interface is needed.

**Alternative considered**: Add `input()` to `StepContext`. Rejected — `StepContext` is already an interface in the public API; adding `input()` would break the current clean separation between the "channel" (typed step input/output) and the "execution environment" (state/context), and would require making `StepContext` generic.

### 2. Arms expressed as pre-built `Pipeline<O, R>` instances

Branch arms are `Pipeline<O, R>` objects, constructed independently via their own `PipelineBuilder` chains and passed to `branch()`. This means:
- Arms are validated at the time their own `build()` is called — wiring errors in an arm surface immediately, not buried in the parent's `build()`.
- Arms can be reused across multiple branch points or pipelines.
- The type contract (`inputType` and `outputType`) is available as class tokens on the `Pipeline` object and can be checked at parent `build()` time.

**Alternative considered**: Accept a `Consumer<PipelineBuilder<O, ?>>` lambda that builds the arm inline. Rejected — error messages from a nested builder are harder to attribute, and arms built inline cannot be reused or tested in isolation.

### 3. Branch identified by a required string ID

`branch(String branchId, ...)` — the branch id is required, not auto-generated. It appears in trace entries, `PipelineBuildException` messages, and potential log events. Auto-generated ids (e.g., `"branch-0"`) make multi-branch pipelines harder to interpret in monitoring.

Duplicate branch ids (or a branch id colliding with any step id anywhere in the pipeline) are rejected at `build()`, consistent with the existing duplicate step id validation.

### 4. Build-time arm type validation

At `build()` time, the engine validates:
- `ifTrue.inputType()` equals the builder's current output type at the branch call site.
- `ifFalse.inputType()` equals the builder's current output type at the branch call site.
- `ifTrue.outputType()` equals `ifFalse.outputType()` — both arms must produce the same type (guaranteed structurally by generics, but checked at runtime against class tokens to guard raw-type escapes).

These checks mirror the existing `.then()` input-type validation.

### 5. Trace semantics: branch entry + taken arm entries + SKIPPED entries

The `ExecutionTrace` for a pipeline containing a branch node contains:
1. A synthetic trace entry for the branch node itself — `stepId = branchId`, `durationNanos` = time taken to evaluate the predicate, `attempts = 1`.
2. Trace entries from the **taken** arm, in execution order, flattened into the parent trace.
3. **SKIPPED** trace entries for every step in the **non-taken** arm, with `durationNanos = 0` and `attempts = 0`.

This gives callers a complete picture of which path was chosen and which steps were bypassed without requiring them to know the pipeline structure in advance. SKIPPED entries appear after the taken arm's entries, in declaration order.

`TraceEntry` must be relaxed to permit `attempts = 0` for SKIPPED entries. The existing `attempts >= 1` invariant is narrowed to: `attempts >= 1` for executed entries; `attempts == 0` for skipped entries. A new boolean accessor `skipped()` (derived from `attempts == 0`) distinguishes the two.

**Alternative considered**: Omit non-taken arm steps from the trace entirely. Rejected — monitoring dashboards that display "all steps in this pipeline" would silently drop non-taken steps, making observability incomplete.

### 6. `StepOutcome.SKIPPED` and metrics emission for non-taken steps

`StepOutcome` gains a third constant: `SKIPPED`. For each step in the non-taken arm:
- The metrics recorder is called with `recordStepDuration(stepId, 0L)`, `recordStepAttempts(stepId, 0)`, and `recordStepOutcome(stepId, StepOutcome.SKIPPED)`.
- A `step.skip` log event is emitted at `DEBUG` level (not `INFO` — skipped steps are structural, not operational events) with `step.id` and `step.branch_id` fields.
- No `step.start`, `step.finish`, or `step.error` events are emitted for skipped steps.

### 7. Predicate failure maps to `Failure` with the branch id

If the predicate throws any `Throwable`, the pipeline terminates as a `Failure` with:
- `cause()` = the thrown `Throwable`
- `failedStepId()` = the branch id

This is consistent with how step exceptions produce `Failure` — the id of the failing "unit" (step or branch) is always recorded.

## Risks / Trade-offs

- **Type-erasure gap on arm output type** — `Pipeline<O, R>` stores `outputType` as a `Class<R>`, but when the generic type `R` is itself generic (e.g., `List<String>`), the class token is only `List.class`. The cross-arm `outputType` equality check catches mismatches at the top level only; inner generic parameters are invisible. This is the same limitation as the existing `.then()` check and is accepted.
  → Mitigation: document clearly; a future schema-validation SPI could provide richer type checks.

- **SKIPPED metrics volume** — in pipelines that branch frequently, the non-taken arm emits as many metric events as the taken arm. This could inflate per-request metric cardinality.
  → Mitigation: The `step.skip` log is DEBUG (easily filtered). Teams with metric overhead concerns can configure a `MetricsRecorder` that ignores `SKIPPED` outcomes.

- **`attempts = 0` breaks `TraceEntry` invariant** — relaxing the invariant to allow `0` for SKIPPED entries is a semantic divergence from the current "attempts is always ≥ 1 for executed steps" contract.
  → Mitigation: The `skipped()` accessor makes this unambiguous without requiring callers to compare integers; the invariant is documented as two cases, not one.

## Open Questions

- Should the predicate receive a read-only `State` view rather than the mutable `State` from `StepContext`? Writing to state inside a predicate is unexpected and could cause subtle bugs. Punting for now — state read/write semantics in step context are unchanged; document that predicate side-effects on state are strongly discouraged.
