### Requirement: foreach applies a step to every element of a list
`PipelineBuilder.foreach(step)` and `PipelineBuilder.foreach(step, concurrency)` SHALL accept a `Step<E, R>` and, when the current pipeline output type is `List`, produce a pipeline whose output type is `List`. Each element of the input list is passed individually to the step, and results are collected into a `List` in the same order as the input.

#### Scenario: All items succeed sequentially
- **WHEN** `foreach(step)` is called on a pipeline whose current output is a `List` of N items, and all N step invocations return successfully
- **THEN** the pipeline returns a `Success` whose value is a `List` of N outputs in input order

#### Scenario: Empty list produces empty output
- **WHEN** `foreach(step)` is called and the runtime input list is empty
- **THEN** the pipeline returns a `Success` whose value is an empty `List`

#### Scenario: Fail-fast on first item failure
- **WHEN** one item's step execution throws, regardless of whether other items have already succeeded
- **THEN** the pipeline returns a `Failure` with the inner step's id as the failing step id, and the exception from that item as the cause; no further items are processed

### Requirement: foreach requires the current pipeline output to be a List
The `foreach` builder method SHALL throw `PipelineBuildException` if the current pipeline output type is not `List`. This check occurs immediately when `foreach` is called, not deferred to `build()`.

#### Scenario: Type mismatch detected at foreach call
- **WHEN** `foreach(step)` is called on a `PipelineBuilder` whose current output type is not `List`
- **THEN** `PipelineBuildException` is thrown with a message identifying the mismatch before `build()` is ever called

#### Scenario: Valid list type accepted
- **WHEN** `foreach(step)` is called on a `PipelineBuilder` whose current output type is `List`
- **THEN** no exception is thrown and the builder continues with output type `List`

### Requirement: foreach default concurrency is sequential
When `foreach(step)` is called without a concurrency argument, items SHALL be processed one at a time, in order, with no thread submission to an `ExecutorService`.

#### Scenario: Items processed in order with concurrency 1
- **WHEN** `foreach(step)` is used without specifying concurrency, and the step records execution order
- **THEN** items are processed in the same order as the input list with no interleaving

### Requirement: foreach supports bounded concurrent execution
`PipelineBuilder.foreach(step, concurrency)` SHALL accept a positive integer `concurrency` and process items up to `concurrency` at a time using the pipeline's `ExecutorService`.

#### Scenario: Concurrent window respects concurrency limit
- **WHEN** `foreach(step, 4)` is called with 10 items and the step records start times
- **THEN** no more than 4 items are in-flight simultaneously at any point in time

#### Scenario: Invalid concurrency rejected at foreach call
- **WHEN** `foreach(step, 0)` or `foreach(step, -1)` is called
- **THEN** `IllegalArgumentException` or `PipelineBuildException` is thrown immediately

#### Scenario: Concurrency larger than list size clamps to list size
- **WHEN** `foreach(step, 10)` is called with a list of 3 items
- **THEN** all 3 items are submitted without error; no more than 3 execute concurrently

### Requirement: foreach emits per-item observability
For each element at index `i`, the framework SHALL emit `step.start`, `step.finish` or `step.error` structured log events and fire all three `MetricsRecorder` methods (`recordStepDuration`, `recordStepAttempts`, `recordStepOutcome`). The step id used for these events SHALL be `<stepId>[<i>]` where `<i>` is the 0-based item index.

#### Scenario: Success emits start and finish per item
- **WHEN** `foreach(step)` processes N items successfully
- **THEN** exactly N `step.start` and N `step.finish` log events are emitted, each carrying the item's indexed id

#### Scenario: Failure emits error for the failing item
- **WHEN** item at index `k` throws
- **THEN** a `step.error` log event is emitted for `<stepId>[k]` and no finish event is emitted for that item

#### Scenario: MetricsRecorder called per item
- **WHEN** `foreach(step)` processes N items successfully
- **THEN** `recordStepDuration`, `recordStepAttempts`, and `recordStepOutcome` are each called exactly N times, once per item

### Requirement: foreach honours per-item retry policy
The `retryPolicy()` defined on the inner step's `StepDescriptor` SHALL be applied independently to each item. Each item's retry attempts are counted separately; an item that exhausts its retries causes fail-fast behaviour for the entire foreach.

#### Scenario: Transient failure retried per item
- **WHEN** the step for item `k` fails on attempt 1 then succeeds on attempt 2, within the configured retry limit
- **THEN** item `k` ultimately contributes its successful result to the output list; the retry attempt is logged and recorded in `ExecutionTrace`

#### Scenario: Item exhausts retries causes failure
- **WHEN** the step for item `k` fails on every attempt up to `maxAttempts`
- **THEN** the pipeline returns `Failure` for that item; remaining items are not processed

### Requirement: foreach records one ExecutionTrace entry per item
Each item's execution SHALL produce a `TraceEntry` in the pipeline's `ExecutionTrace` with id `<stepId>[<index>]`, the start time in nanoseconds, the total duration in nanoseconds (including retry delays), the number of attempts made, and `skipped=false`.

#### Scenario: Trace entries appear in index order
- **WHEN** `foreach(step)` processes N items sequentially
- **THEN** `ExecutionTrace` contains N entries with ids `<stepId>[0]` through `<stepId>[N-1]` in that order

#### Scenario: Failed item trace entry records attempt count
- **WHEN** an item fails after exhausting all retry attempts
- **THEN** the corresponding `TraceEntry` records the actual number of attempts made (equal to `maxAttempts`)

### Requirement: foreach step id participates in duplicate-id detection
The inner step's id SHALL be registered in the pipeline's id-uniqueness check performed at `build()`. If another step in the pipeline declares the same id, `build()` SHALL throw `PipelineBuildException`.

#### Scenario: Duplicate id between foreach step and sibling step rejected at build
- **WHEN** a foreach step and a `.then()` step share the same id
- **THEN** `build()` throws `PipelineBuildException` listing the duplicate id

### Requirement: foreach composes with other pipeline operators
`foreach` SHALL be usable in combination with `.then()`, `.branch()`, `parallel*`, and other `foreach` calls within the same pipeline, subject to type compatibility.

#### Scenario: foreach followed by then
- **WHEN** a pipeline calls `.foreach(step)` and then `.then(aggregatorStep)` where the aggregator's input type is `List`
- **THEN** the pipeline builds and executes successfully, passing the `List` output of foreach to the aggregator

#### Scenario: then followed by foreach
- **WHEN** a pipeline calls `.then(listProducingStep)` and then `.foreach(itemStep)`
- **THEN** the pipeline builds and executes successfully
