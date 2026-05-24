## 1. Engine node: ForeachNode

- [x] 1.1 Create `ForeachNode<E, R>` as a package-private record in `io.flowpipe.engine`, storing `Step<E, R> step` and `int concurrency`
- [x] 1.2 Extend `EngineNode` sealed interface `permits` clause to include `ForeachNode`

## 2. Builder: foreach overloads

- [x] 2.1 Add `foreach(Step<E, R> step)` method to `PipelineBuilder` that checks `currentOutputType.equals(List.class)` and throws `PipelineBuildException` if not, then appends a `ForeachNode` with `concurrency=1`
- [x] 2.2 Add `foreach(Step<E, R> step, int concurrency)` overload that additionally validates `concurrency >= 1` and throws `IllegalArgumentException` or `PipelineBuildException` if not
- [x] 2.3 Extend `PipelineBuilder.validate()` / `collectIds` to register the inner step's id from `ForeachNode` for duplicate-id detection

## 3. Engine execution: retry helper refactor

- [x] 3.1 Extract the inline retry loop from `Pipeline.executeShared` (the `StepNode` branch) into a private `executeItemWithRetry(step, input, itemLabel, ctx, context, recorder, traceBuilder)` method that encapsulates retry, observability, and trace entry creation
- [x] 3.2 Update the `StepNode` branch in `executeShared` to call `executeItemWithRetry` with `itemLabel = stepId`

## 4. Engine execution: foreach dispatch

- [x] 4.1 Add `ForeachNode` branch to `Pipeline.executeShared` that casts current input to `List<?>`
- [x] 4.2 Implement sequential path (concurrency=1): iterate the list and call `executeItemWithRetry` for each item with `itemLabel = stepId + "[" + index + "]"`; collect results; return `Failure` on first item failure
- [x] 4.3 Implement concurrent path (concurrency>1): partition the list into windows of size `concurrency`; for each window submit items as `Callable`s to the pipeline executor; collect futures in order; cancel remaining on first failure; return `Failure` or advance to next window

## 5. Observability: per-item log events

- [x] 5.1 Confirm that `executeItemWithRetry` emits `step.start`, `step.finish`, `step.error`, and `step.retry` events using `itemLabel` as the step id (these come for free from the extracted helper if `emitStart`/`emitFinish`/`emitError` accept a label parameter)
- [x] 5.2 Confirm `MetricsRecorder` is called per item via `emitRecord` inside `executeItemWithRetry`

## 6. Tests

- [x] 6.1 Write `ForeachExecutionTest` in `flowpipe-core` covering: all-success sequential, empty list, fail-fast on first item failure, retry per item (transient failure then success), item that exhausts retries
- [x] 6.2 Add concurrency>1 test: verify results are in input order; verify fail-fast cancels remaining items
- [x] 6.3 Add build-time tests: `foreach` on non-List output type throws `PipelineBuildException`; invalid concurrency argument throws; duplicate step id detected at `build()`
- [x] 6.4 Add composition test: `then → foreach → then` and `foreach → foreach` pipelines build and execute correctly
- [x] 6.5 Add observability test: `MetricsRecorder` called once per item; trace contains N entries with indexed ids

## 7. Test utilities and example

- [x] 7.1 Extend `Steps` utility in `flowpipe-test` with at least one list-producing step and one per-element step for use in integration tests
- [x] 7.2 Add a foreach usage example to `OrderProcessingPipeline` or a standalone example class in `flowpipe-test/example`

## 8. Validation

- [x] 8.1 Run `./gradlew build` and confirm all tests pass with zero warnings (`-Xlint:all -Werror`)
