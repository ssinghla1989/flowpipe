## 1. StepOutcome and TraceEntry Schema Changes

- [x] 1.1 Add `SKIPPED` constant to `StepOutcome` enum in `io.flowpipe.observability`
- [x] 1.2 Add `boolean skipped()` accessor to `TraceEntry` record and relax the `attempts >= 1` compact constructor invariant to permit `attempts == 0` when `skipped() == true`
- [x] 1.3 Update `NoOpMetricsRecorder` to handle `SKIPPED` (already a no-op, but verify no switch exhaustiveness warnings)
- [x] 1.4 Update `RecordingMetricsRecorder` in `flowpipe-test` to record and expose `SKIPPED` outcome events

## 2. BranchNode Engine Type

- [x] 2.1 Add `BranchNode<I, O, R>` as a package-private sealed type in `io.flowpipe.engine` that extends `EngineNode<I, R>` and holds: `branchId`, `BiPredicate<O, StepContext>`, `Pipeline<O, R> ifTrue`, `Pipeline<O, R> ifFalse`
- [x] 2.2 Extend the `EngineNode` sealed interface to permit `BranchNode`

## 3. PipelineBuilder.branch() Method

- [x] 3.1 Add `branch(String branchId, BiPredicate<O, StepContext> predicate, Pipeline<O, R> ifTrue, Pipeline<O, R> ifFalse)` method to `PipelineBuilder` — null-check all args, append `BranchNode` to nodes, return a new `PipelineBuilder<I, R>` with the output type taken from `ifTrue.outputType()`
- [x] 3.2 Extend `PipelineBuilder.validate()` to collect branch ids into the duplicate-id scan (`collectIds` helper)
- [x] 3.3 Extend `PipelineBuilder.validate()` to check `ifTrue.inputType()` and `ifFalse.inputType()` match the builder's output type at the branch call site, and that `ifTrue.outputType()` equals `ifFalse.outputType()`; throw `PipelineBuildException` with branch id and type names on mismatch

## 4. Pipeline Execution Engine — Branch Handling

- [x] 4.1 Add a branch execution path in `Pipeline` (the engine loop): when a `BranchNode` is encountered, record predicate start time, invoke `predicate.test(currentValue, stepContext)`, record predicate duration, and append a synthetic `TraceEntry(branchId, startNanos, durationNanos, 1)` with `skipped() == false`
- [x] 4.2 Execute the selected arm pipeline (call its internal step-execution logic, not its public `execute()`, to share the parent's `State`, `RequestContext`, and `MetricsRecorder`); flatten the arm's trace entries into the parent trace
- [x] 4.3 For each step in the non-taken arm, append a SKIPPED `TraceEntry` with `durationNanos = 0`, `attempts = 0`, `skipped() == true`; call `recorder.recordStepDuration(id, 0L)`, `recorder.recordStepAttempts(id, 0)`, `recorder.recordStepOutcome(id, StepOutcome.SKIPPED)` for each; emit a `step.skip` SLF4J DEBUG log with `step.id` and `step.branch_id` fields
- [x] 4.4 If the predicate throws, terminate the pipeline with `Failure(cause, branchId, traceUpToHere)`

## 5. Tests — Conditional Routing

- [x] 5.1 Test: `branch()` with matching types compiles and returns a builder with the correct output type
- [x] 5.2 Test: `build()` rejects mismatched `ifTrue` input type — asserts exception message contains branch id and type names
- [x] 5.3 Test: `build()` rejects mismatched `ifFalse` input type
- [x] 5.4 Test: `build()` rejects mismatched arm output types
- [x] 5.5 Test: `build()` rejects branch id duplicating a sequential step id
- [x] 5.6 Test: `build()` rejects two branch nodes with the same id
- [x] 5.7 Test: predicate returning `true` routes to `ifTrue` arm — asserts correct output value
- [x] 5.8 Test: predicate returning `false` routes to `ifFalse` arm — asserts correct output value
- [x] 5.9 Test: predicate reads shared state to make routing decision
- [x] 5.10 Test: throwing predicate produces `Failure` with `failedStepId == branchId`
- [x] 5.11 Test: trace contains branch node entry, taken arm entries, and SKIPPED entries for non-taken arm in correct order
- [x] 5.12 Test: SKIPPED trace entries have `durationNanos == 0`, `attempts == 0`, `skipped() == true`
- [x] 5.13 Test: `RecordingMetricsRecorder` receives `SKIPPED` outcome with duration `0` and attempts `0` for non-taken steps
- [x] 5.14 Test: `step.skip` DEBUG log is emitted for each non-taken step with `step.id` and `step.branch_id` fields
- [x] 5.15 Test: nested branch — outer selects `ifTrue`, inner selects `ifFalse`; trace entries are flattened in correct depth-first order
- [x] 5.16 Test: step failure inside taken arm produces `Failure` with that step's id (not the branch id)
