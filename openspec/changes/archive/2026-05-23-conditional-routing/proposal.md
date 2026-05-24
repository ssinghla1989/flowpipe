## Why

FlowPipe pipelines currently execute steps in a fixed linear or parallel sequence, with no way to route execution based on the result of a step or the value of shared state. Real-world API flows routinely need to branch — e.g., skip downstream enrichment when an upstream lookup returns nothing, or fan out to different processing paths based on response type — and today those decisions must live outside the pipeline in the calling code, defeating the purpose of the framework.

## What Changes

- New `Branch` step type that evaluates a predicate against a `StepContext` and selects one of two typed sub-pipelines to execute.
- `PipelineBuilder` gains a `branch(predicate, ifTrue, ifFalse)` factory method that wires a conditional node between existing pipeline steps.
- `branch()` is validated at `pipeline.build()` time: both arms must have compatible output types and must form complete, validatable sub-pipelines.
- `ExecutionTrace` extended to record which branch arm was taken on each execution.
- `StepOutcome` enum gains a `SKIPPED` value so steps in the arm not taken can be represented in the trace without being executed.

## Capabilities

### New Capabilities

- `conditional-routing`: Typed conditional branching — a step-level construct that evaluates a predicate and delegates to one of two sub-pipelines, with full observability and build-time type safety.

### Modified Capabilities

- `pipeline-composition`: `PipelineBuilder` gains a new composition method (`branch`); build-time validation rules expand to cover branch-arm type compatibility.
- `pipeline-result`: `ExecutionTrace` / `TraceEntry` must record the branch arm taken (or skipped) so callers can reconstruct what executed.
- `step-observability`: `StepOutcome` gains a `SKIPPED` variant; observability emission for skipped steps must be defined.

## Impact

- **`flowpipe-core`** — new `BranchNode` engine node (package-private, sealed); `PipelineBuilder` new method; `ExecutionTrace`/`TraceEntry` schema change; `StepOutcome` enum change.
- **`flowpipe-test`** — `RecordingMetricsRecorder` and `StepHarness` may need to handle `SKIPPED` outcome; test helpers for asserting branch paths.
- **Public API surface** — `PipelineBuilder.branch(...)` is new public API; `StepOutcome.SKIPPED` is a new enum constant; `TraceEntry` may gain a new field. No existing public methods are removed or renamed (**non-breaking**).
- **No new runtime dependencies.**
