# parallel-step-execution

## Purpose

Defines how a parallel block dispatches multiple steps concurrently against the same input, synchronises their results, merges outputs via a typed combiner, surfaces failures, and emits per-branch observability identical to sequential step execution.

## Requirements

### Requirement: Parallel block runs all branches concurrently against the same input

The engine SHALL dispatch each step in a parallel block to an `ExecutorService` and run all branches simultaneously. Each branch SHALL receive the same input value (the current pipeline cursor value at the point the parallel block begins). The engine SHALL NOT start any subsequent pipeline step until all branches have either completed or failed.

#### Scenario: Two branches receive the same input concurrently

- **WHEN** a pipeline executes a two-branch parallel block whose input is the string `"hello"` and both branches record their received input
- **THEN** both branches MUST observe `"hello"` as their input, and both MUST have been invoked before any step after the parallel block runs

### Requirement: Typed combiner merges branch outputs into a single value

For typed parallel overloads (arity 2–4), the developer supplies a typed combiner function (e.g., `BiFunction<A, B, R>` for arity 2, `TriFunction<A, B, C, R>` for arity 3, `QuadFunction<A, B, C, D, R>` for arity 4). The engine SHALL invoke the combiner with branch outputs in the order the branches were declared (not the order they completed). The combiner's return value becomes the parallel block's output and the next step's input.

#### Scenario: Combiner receives branch outputs in declaration order

- **WHEN** a two-branch parallel block with `stepA` and `stepB` executes, `stepA` returns `"A"`, `stepB` returns `"B"`, and the combiner is `(a, b) -> a + b`
- **THEN** the combiner MUST be called with `"A"` as first argument and `"B"` as second argument, and the result MUST be `"AB"`

### Requirement: Any branch failure fails the entire parallel block

If any branch throws any `Throwable` (including from input/output validation), the engine SHALL cancel remaining in-flight branches (best-effort via `Future.cancel(true)`) and return a `Failure` result. The `Failure.failedStepId()` SHALL equal the id of the branch step that threw. No subsequent pipeline steps SHALL run.

#### Scenario: One failing branch produces Failure with that branch's step id

- **WHEN** a two-branch parallel block has `stepA` completing successfully and `stepB` throwing `RuntimeException("boom")`
- **THEN** the result MUST be a `Failure` with `failedStepId()` equal to `stepB`'s id, and any step after the parallel block MUST NOT be invoked

#### Scenario: Failure cause is the branch's original exception

- **WHEN** a branch throws `new IllegalStateException("root cause")`
- **THEN** the `Failure.cause()` MUST be that `IllegalStateException` (not a wrapping `ExecutionException` or similar)

### Requirement: Per-branch observability is identical to sequential step observability

The engine SHALL emit `step.start`, then either `step.finish` or `step.error`, for each branch individually — exactly as it does for sequential steps. The `MetricsRecorder` SHALL receive `recordStepDuration`, `recordStepAttempts`, and `recordStepOutcome` calls for each branch. All observability events from parallel branches carry the individual branch step's id, not a group id.

#### Scenario: Both branches produce their own step.start and step.finish events

- **WHEN** a two-branch parallel block executes successfully with branches `stepA` and `stepB` and a `Slf4jTestAppender` is attached
- **THEN** exactly one `step.start` event with `step.id="stepA"` MUST be captured, one with `step.id="stepB"`, one `step.finish` with `step.id="stepA"`, and one `step.finish` with `step.id="stepB"`

#### Scenario: Failing branch produces step.error, not step.finish

- **WHEN** a branch with id `"unstable"` throws and the parallel block fails
- **THEN** exactly one `step.error` event with `step.id="unstable"` MUST be captured and no `step.finish` event with `step.id="unstable"` MUST be captured

### Requirement: ExecutionTrace includes an entry for each completed or failing branch

The `ExecutionTrace` returned with the `Result` SHALL contain one `TraceEntry` per branch that ran. On success, all branches produce a `TraceEntry`. On failure, the failing branch and any branches that completed before it produce `TraceEntry`s; branches that were cancelled after the failure MAY or MAY NOT appear. Trace order within a parallel block is not guaranteed.

#### Scenario: Success trace contains entries for all branches

- **WHEN** a two-branch parallel block executes successfully
- **THEN** the resulting `Success.trace().entries()` MUST contain at least two `TraceEntry`s whose `stepId` values correspond to the two branches (in any order)

### Requirement: Executor lifecycle is wholly owned by the caller

The engine SHALL use the `ExecutorService` supplied by the caller (or `ForkJoinPool.commonPool()` if none is supplied) for branch dispatch. The engine SHALL NEVER shut down, await termination of, or otherwise manage the lifecycle of the executor. If the executor is shut down at the time of dispatch, the resulting `RejectedExecutionException` SHALL be surfaced as a `Failure` for the affected branch.

#### Scenario: Shut-down executor causes Failure, not engine crash

- **WHEN** a pipeline is built with a custom executor that has been shut down, and a parallel block is executed
- **THEN** the result MUST be a `Failure` and the pipeline execution MUST return normally (no unchecked exception propagates out of `Pipeline.execute(...)`)

### Requirement: Resilience policies on parallel branch steps are honored

`RetryPolicy`, `TimeoutPolicy`, and `CircuitBreakerPolicy` attached to a parallel branch step's `StepDescriptor` SHALL be applied to that branch's execution exactly as they would be for a sequential step. The engine SHALL NOT treat parallel branches as a special case that bypasses resilience machinery.

#### Scenario: Retry policy on a parallel branch step recovers a transient failure

- **WHEN** a parallel branch step has a `RetryPolicy` with `maxAttempts=3` and its `execute` method fails on the first two calls then succeeds on the third
- **THEN** the branch MUST produce a successful result after 3 invocations, and the combined parallel block output MUST reflect the successful branch output

#### Scenario: Circuit breaker on a parallel branch step fast-fails when open

- **WHEN** a parallel branch step has a `CircuitBreakerPolicy` and the circuit has been opened by prior failures
- **THEN** the branch MUST fast-fail with `CircuitBreakerOpenException` without invoking `execute`, and the parallel block MUST return a `Failure`

### Requirement: Pipeline deadline is enforced while waiting for parallel branch futures

If the pipeline has a configured deadline, the engine SHALL enforce it while collecting parallel branch results. The engine SHALL NOT block indefinitely on a slow branch when the deadline has passed.

#### Scenario: Slow parallel branch does not block past the pipeline deadline

- **WHEN** a pipeline with a deadline has a parallel block where one branch sleeps significantly longer than the deadline
- **THEN** the pipeline MUST return a `Failure` with `cause()` instanceof `PipelineDeadlineExceededException` and `failedStepId()` equal to `"pipeline.deadline"` without waiting for the slow branch to finish

### Requirement: parallelN provides a variadic escape hatch for arities above 4

The builder SHALL expose a `parallelN(Class<R> resultType, Map<String, Step<I, ?>> steps, Function<Map<String, Object>, R> combiner)` method. The map keys MUST match the corresponding step's `StepDescriptor.id()`; if any key does not match, `build()` SHALL throw `PipelineBuildException`. The combiner receives a `Map<String, Object>` keyed by step id with each branch's output as the value.

#### Scenario: parallelN combiner receives a map keyed by step ids

- **WHEN** a `parallelN` block has two steps with ids `"x"` and `"y"` returning `1` and `2` respectively, and the combiner sums the values
- **THEN** the combiner MUST be called with a map containing `{"x": 1, "y": 2}` and MUST return `3`

#### Scenario: parallelN rejects a map key that disagrees with the step descriptor id

- **WHEN** a developer calls `parallelN` and passes a map key `"alias"` for a step whose descriptor id is `"real-name"`
- **THEN** `build()` MUST throw `PipelineBuildException` whose message mentions the mismatch
