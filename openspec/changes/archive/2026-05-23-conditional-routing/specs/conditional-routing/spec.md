## ADDED Requirements

### Requirement: PipelineBuilder exposes a typed branch method

The builder SHALL expose a method `branch(String branchId, BiPredicate<O, StepContext> predicate, Pipeline<O, R> ifTrue, Pipeline<O, R> ifFalse)` that returns a `PipelineBuilder<I, R>`. The `branchId` MUST be non-null and non-blank. Both arm pipelines MUST be non-null. The predicate MUST be non-null. Passing null for any argument SHALL throw `NullPointerException`.

#### Scenario: branch method accepts valid arguments and advances the output type

- **WHEN** a developer calls `.branch("check", (val, ctx) -> val > 0, positivePipeline, negativePipeline)` on a `PipelineBuilder<String, Integer>` where both arm pipelines are `Pipeline<Integer, String>`
- **THEN** the call MUST compile and the returned builder MUST have type `PipelineBuilder<String, String>`

### Requirement: build() validates branch arm input types match the pipeline's current output type

At `build()` time, the engine SHALL verify that both `ifTrue.inputType()` and `ifFalse.inputType()` equal the builder's current output type at the `branch()` call site. If either arm's input type does not match, `build()` SHALL throw `PipelineBuildException` whose message names the branch id, the offending arm ("ifTrue" or "ifFalse"), the expected type, and the actual type.

#### Scenario: mismatched ifTrue input type is rejected at build time

- **WHEN** a pipeline is built with `.branch("route", pred, Pipeline<Boolean, String>, Pipeline<Integer, String>)` but the builder's current output type is `Integer`
- **THEN** `build()` MUST throw `PipelineBuildException` whose message contains `"route"`, `"ifTrue"`, and both type names

#### Scenario: mismatched ifFalse input type is rejected at build time

- **WHEN** a pipeline is built with `.branch("route", pred, Pipeline<Integer, String>, Pipeline<Boolean, String>)` where the builder's current output type is `Integer`
- **THEN** `build()` MUST throw `PipelineBuildException` whose message contains `"route"`, `"ifFalse"`, and both type names

### Requirement: build() validates that both branch arms produce the same output type

At `build()` time, the engine SHALL verify that `ifTrue.outputType()` equals `ifFalse.outputType()`. If they differ, `build()` SHALL throw `PipelineBuildException` whose message names the branch id and both mismatched output types.

#### Scenario: arms with different output types are rejected at build time

- **WHEN** a pipeline is built with `.branch("split", pred, Pipeline<Integer, String>, Pipeline<Integer, Long>)`
- **THEN** `build()` MUST throw `PipelineBuildException` whose message contains `"split"`, `"String"`, and `"Long"`

### Requirement: build() rejects duplicate branch ids and branch ids that collide with step ids

The existing duplicate-id check SHALL be extended to include branch ids. A branch id that duplicates any step id (sequential or parallel), any other branch id, or itself (if the same branch object appears twice) SHALL cause `build()` to throw `PipelineBuildException` whose message lists the duplicated id(s).

#### Scenario: branch id that matches a sequential step id is rejected

- **WHEN** a pipeline is built with a sequential step whose id is `"enrich"` and a branch node whose `branchId` is also `"enrich"`
- **THEN** `build()` MUST throw `PipelineBuildException` whose message contains `"enrich"`

### Requirement: Predicate is evaluated against the flowing value and the current StepContext

At runtime, when execution reaches the branch node, the engine SHALL invoke `predicate.test(currentValue, stepContext)` where `currentValue` is the output of the preceding node and `stepContext` exposes the current mutable `State` and immutable `RequestContext`. The predicate result SHALL determine which arm executes: `true` routes to `ifTrue`, `false` routes to `ifFalse`.

#### Scenario: predicate returning true routes execution to ifTrue arm

- **WHEN** a branch node has predicate `(val, ctx) -> val > 0` and the incoming value is `5`
- **THEN** `ifTrue` arm MUST be executed and `ifFalse` arm MUST NOT be executed

#### Scenario: predicate returning false routes execution to ifFalse arm

- **WHEN** a branch node has predicate `(val, ctx) -> val > 0` and the incoming value is `-3`
- **THEN** `ifFalse` arm MUST be executed and `ifTrue` arm MUST NOT be executed

#### Scenario: predicate can access shared state to inform routing decision

- **WHEN** a branch node has predicate `(val, ctx) -> ctx.state().get(MY_KEY).isPresent()` and the shared state contains `MY_KEY`
- **THEN** the predicate MUST evaluate to `true` and `ifTrue` arm MUST be executed

### Requirement: Predicate failure terminates the pipeline as Failure with the branch id

If `predicate.test(...)` throws any `Throwable`, the engine SHALL terminate the pipeline immediately with a `Failure` where `cause()` is the thrown `Throwable` and `failedStepId()` equals the branch id. The `ifTrue` and `ifFalse` arms SHALL NOT be executed.

#### Scenario: throwing predicate produces Failure with branch id as failedStepId

- **WHEN** a branch node with id `"route"` has a predicate that throws `new IllegalStateException("bad state")`
- **THEN** the result MUST be a `Failure`, `failedStepId()` MUST equal `"route"`, and `cause()` MUST be the `IllegalStateException`

### Requirement: Taken arm steps are executed and appear in the trace; non-taken arm steps appear as SKIPPED entries

When the branch predicate selects an arm, the engine SHALL execute every step in the selected arm and append their `TraceEntry` instances to the pipeline's trace. For every step in the non-selected arm, the engine SHALL append a SKIPPED `TraceEntry` (with `durationNanos = 0`, `attempts = 0`, and `skipped() == true`) after the taken arm's entries. The branch node itself SHALL contribute a synthetic `TraceEntry` with `stepId` equal to the branch id immediately before the arm entries.

#### Scenario: taken arm steps appear in trace after branch node entry

- **WHEN** a branch with id `"route"` selects `ifTrue` which contains steps `"a"` and `"b"`, and `ifFalse` contains step `"c"`
- **THEN** the trace MUST contain entries in order: `"route"` (branch node), `"a"`, `"b"`, `"c"` (SKIPPED)
- **AND** the `"c"` entry MUST have `skipped() == true` and `durationNanos == 0`

### Requirement: Nested branches are supported

A `Pipeline` passed as an arm to `branch()` MAY itself contain branch nodes. The engine SHALL execute nested branches with the same semantics, and nested trace entries SHALL be flattened into the parent trace in depth-first order.

#### Scenario: nested branch resolves and its trace entries appear in the outer trace

- **WHEN** a pipeline has a branch with id `"outer"` whose `ifTrue` arm is a pipeline containing a branch with id `"inner"`, and `"outer"` selects `ifTrue`, and `"inner"` selects its `ifFalse`
- **THEN** the outer trace MUST contain entries for `"outer"` (branch node), `"inner"` (branch node), the taken `"inner.ifFalse"` steps, the skipped `"inner.ifTrue"` steps, and the skipped `"outer.ifFalse"` steps
