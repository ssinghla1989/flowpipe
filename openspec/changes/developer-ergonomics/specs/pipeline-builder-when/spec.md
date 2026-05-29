## ADDED Requirements

### Requirement: PipelineBuilder.when() applies a step conditionally
`PipelineBuilder<I, O>` SHALL provide a method `when(String id, BiPredicate<O, StepContext> predicate, Step<O, O> step)` that adds a conditional node: when `predicate` returns `true`, the step is executed; when `predicate` returns `false`, the current pipeline value passes through unchanged. The output type of the builder MUST remain `O` after calling `.when()`. The method SHALL be implemented as a pass-through-armed branch internally, using `id` as the branch id.

#### Scenario: Predicate true — step executes
- **WHEN** `.when("guard", (v, ctx) -> v > 0, doubleStep)` is in the pipeline and the input value is positive
- **THEN** `doubleStep` executes and its output becomes the pipeline value

#### Scenario: Predicate false — step is skipped, input passes through
- **WHEN** `.when("guard", (v, ctx) -> v > 0, doubleStep)` is in the pipeline and the input value is zero or negative
- **THEN** `doubleStep` is NOT executed and the original value passes through unchanged to the next stage

#### Scenario: Output type is unchanged after .when()
- **WHEN** `.when("guard", predicate, stepOfTypeOO)` is called on a `PipelineBuilder<I, O>`
- **THEN** the returned builder is still `PipelineBuilder<I, O>` and `.then(Step<O, X>)` can be chained after it

#### Scenario: Observability: skipped step appears in trace with skipped=true
- **WHEN** the predicate is false and the step is not executed
- **THEN** the `ExecutionTrace` contains an entry for the step with `skipped() == true` and `durationNanos() == 0`

#### Scenario: Observability: executed step appears in trace with skipped=false
- **WHEN** the predicate is true and the step executes successfully
- **THEN** the `ExecutionTrace` contains an entry for the step with `skipped() == false` and a positive `durationNanos()`

#### Scenario: Throwing predicate surfaces as Failure with the when-id
- **WHEN** the predicate throws an exception
- **THEN** the pipeline returns a `Failure` with `failedStepId` equal to the `id` passed to `.when()`

#### Scenario: Failing step surfaces as Failure with the step's id
- **WHEN** the predicate is true and the step throws an exception
- **THEN** the pipeline returns a `Failure` with `failedStepId` equal to the step's own id, not the `when` id

### Requirement: .when() id is validated as non-blank and unique within the pipeline
The `id` argument to `.when()` SHALL follow the same uniqueness and non-blank constraints as any branch id: calling `.build()` with a duplicate id or blank id SHALL throw `PipelineBuildException`.

#### Scenario: Duplicate when-id detected at build time
- **WHEN** two `.when()` calls in the same pipeline use the same id
- **THEN** `pipeline.build()` throws `PipelineBuildException` mentioning the duplicate id

#### Scenario: Blank when-id rejected immediately
- **WHEN** `.when("")` is called
- **THEN** `IllegalArgumentException` is thrown before `.build()` is reached
