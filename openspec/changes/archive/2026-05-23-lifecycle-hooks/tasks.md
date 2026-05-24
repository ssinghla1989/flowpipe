## 1. PipelineLifecycle SPI

- [x] 1.1 Create `io.flowpipe.api.PipelineLifecycle<I, O>` interface with default no-op `onStart(I input, StepContext ctx)`, `onFinish(Result<O> result, StepContext ctx)`, and `onError(Failure<O> failure, StepContext ctx)` methods
- [x] 1.2 Verify the interface compiles with an anonymous `new PipelineLifecycle<>() {}` (no overrides)

## 2. PipelineBuilder Integration

- [x] 2.1 Add `private PipelineLifecycle<I, O> lifecycle` field to `PipelineBuilder`, initialized to a no-op lambda or anonymous instance
- [x] 2.2 Add `withLifecycle(PipelineLifecycle<I, O> lifecycle)` method: null-check, `ensureUsable()`, store, return `this`
- [x] 2.3 Thread the `lifecycle` field through the private `PipelineBuilder` constructors so it propagates across chained builder calls (`.then(...)`, `.branch(...)`, etc.)
- [x] 2.4 Pass the resolved `lifecycle` to `new Pipeline<>(...)` in `build()`

## 3. Pipeline Engine Dispatch

- [x] 3.1 Add `private final PipelineLifecycle<I, O> lifecycle` field to `Pipeline` and update its package-private constructor to accept it
- [x] 3.2 In `Pipeline.execute(I input, RequestContext context, MetricsRecorder recorder)`: call `lifecycle.onStart(input, ctx)` before `executeShared`; wrap the call in try/catch — if it throws, return `new Failure<>(t, "pipeline.onStart", traceBuilder.build())` without calling `onFinish` or `onError`
- [x] 3.3 After `executeShared` returns `result`: call `lifecycle.onFinish(result, ctx)` in a try/catch that logs warnings on exception (matching the `safeRecord` pattern)
- [x] 3.4 If result is a `Failure`, additionally call `lifecycle.onError((Failure<O>) result, ctx)` in the same isolated try/catch after `onFinish`
- [x] 3.5 Confirm `executeShared` is NOT modified — hooks must not fire for branch-arm or parallel sub-pipeline calls

## 4. Test Utility

- [x] 4.1 Create `io.flowpipe.test.RecordingPipelineLifecycle<I, O>` in `flowpipe-test` that records each invocation with its arguments in insertion order
- [x] 4.2 Expose `onStartInvocations()`, `onFinishInvocations()`, and `onErrorInvocations()` accessors returning unmodifiable lists of recorded call records

## 5. Tests

- [x] 5.1 Write `LifecycleHooksTest` in `flowpipe-core` covering: `onStart` fires before the first step; `onFinish` fires with `Success` result; `onFinish` fires with `Failure` result; `onError` fires after `onFinish` only on failure; `onStart` exception halts execution and returns `Failure("pipeline.onStart")` without calling `onFinish`/`onError`; `onFinish` exception does not change the result and does not suppress `onError`; `onError` exception does not change the failure result
- [x] 5.2 Write `LifecycleNotCalledForSubPipelinesTest` confirming `onStart` is not called for branch-arm sub-pipelines
- [x] 5.3 Write `WithLifecycleBuilderTest` covering: `null` argument throws NPE; calling on consumed builder throws ISE; default no-lifecycle pipeline executes without error; subsequent `.withLifecycle(...)` calls replace the previous instance

## 6. CLAUDE.md Update

- [x] 6.1 Update the Status section of `CLAUDE.md` to note that lifecycle hooks have landed, removing it from the "not yet implemented" list
