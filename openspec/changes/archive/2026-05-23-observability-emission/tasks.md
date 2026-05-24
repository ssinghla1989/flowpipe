## 1. Observability SPI in `flowpipe-core`

- [x] 1.1 Create package `io.flowpipe.observability`
- [x] 1.2 Define `StepOutcome` enum with values `SUCCESS` and `FAILURE`
- [x] 1.3 Define `MetricsRecorder` interface with `recordStepDuration(String, long)`, `recordStepAttempts(String, int)`, `recordStepOutcome(String, StepOutcome)`
- [x] 1.4 Implement `NoOpMetricsRecorder` as a stateless singleton with a typed `instance()` accessor, methods doing nothing

## 2. Wire observability into the engine

- [x] 2.1 Add a private `MetricsRecorder defaultRecorder` field to `Pipeline<I, O>`, defaulting to `NoOpMetricsRecorder.instance()`
- [x] 2.2 Add `PipelineBuilder.withMetrics(MetricsRecorder)` that stores the recorder; null throws NPE; subsequent calls replace; passed through to the constructed `Pipeline` at `build()`
- [x] 2.3 Add `Pipeline.execute(I input, RequestContext context, MetricsRecorder recorder)` overload that uses the supplied recorder for that call only; the existing two-arg and one-arg overloads continue to use the default
- [x] 2.4 Add a private SLF4J `Logger` to `Pipeline` (or a sibling engine class) via `LoggerFactory.getLogger(...)`
- [x] 2.5 Refactor the per-step execution body so it: emits `step.start` (with `step.id`, `step.attempt=1`, and every `RequestContext` entry as a KV), runs validation + execute + validation, computes `durationNanos`, appends `TraceEntry`, calls `recorder.recordStepDuration`, `recordStepAttempts`, `recordStepOutcome` each inside its own try/catch, then emits `step.finish` on success or `step.error` on failure with the documented fields
- [x] 2.6 Implement recorder-call exception isolation: any `Throwable` from a recorder method MUST be caught, logged at WARN with message `metrics.recorder_failed` and fields `step.id`, `error.class`, `error.message`, and MUST NOT change the pipeline outcome
- [x] 2.7 Ensure `step.finish`'s `step.duration_ms` and the `TraceEntry.durationNanos` and `MetricsRecorder.recordStepDuration` all derive from the same `nanoTime` measurement (no drift)

## 3. Test infrastructure in `flowpipe-test`

- [x] 3.1 Implement `RecordingMetricsRecorder` capturing every `recordStepDuration` / `recordStepAttempts` / `recordStepOutcome` call as an immutable event record; expose `events()` returning the ordered list and `events(String stepId)` filtered helper
- [x] 3.2 Add a `RecordingMetricsRecorderTest` smoke test verifying capture works

## 4. SLF4J test plumbing in `flowpipe-core`

- [x] 4.1 Add Logback Classic (`ch.qos.logback:logback-classic`) as a `testImplementation` of `flowpipe-core` so SLF4J emissions are observable; pin a version compatible with `slf4j-api:2.0.13`
- [x] 4.2 Create a test-only `Slf4jTestAppender` (Logback `ListAppender<ILoggingEvent>` subclass) that captures log events including their `KeyValuePair` structured fields, with helpers to attach to the engine logger and to clear/drain between tests
- [x] 4.3 Add a `logback-test.xml` under `flowpipe-core/src/test/resources/` configuring console output at WARN and leaving the engine logger reachable for in-test appender attachment

## 5. Spec-driven test coverage in `flowpipe-core`

- [x] 5.1 `step.start` log carries `step.id`, `step.attempt=1`, and every `RequestContext` entry as KVs
- [x] 5.2 `step.finish` log on success carries `step.id`, `step.outcome="success"`, and `step.duration_ms >= 0`
- [x] 5.3 `step.error` log on failure carries `step.id`, `step.outcome="failure"`, `step.error_class`, `step.error_message`, is emitted at `ERROR` level, and no `step.finish` is emitted for the failing step
- [x] 5.4 `NoOpMetricsRecorder` methods are silent no-ops
- [x] 5.5 Builder default is the no-op recorder when `.withMetrics(...)` is not called (asserted by routing a per-call override against a defaulted pipeline and observing zero events on the default path)
- [x] 5.6 `.withMetrics(a).withMetrics(b)` causes only `b` to receive emissions
- [x] 5.7 Per-call recorder override receives emissions only for that call; the build-time default receives subsequent calls
- [x] 5.8 Success path: recorder observes one duration, one attempts=1, one outcome=SUCCESS per step
- [x] 5.9 Failure path: recorder observes one duration, one attempts=1, one outcome=FAILURE for the failing step (and SUCCESS for any prior step)
- [x] 5.10 Recorder `durationNanos` equals the corresponding `TraceEntry.durationNanos`
- [x] 5.11 Throwing recorder does not turn a Success into a Failure; a `metrics.recorder_failed` WARN log is emitted per failing recorder call
- [x] 5.12 Throwing recorder does not mask a genuine step failure; the `Failure.cause()` is the step's thrown exception, not the recorder's

## 6. Architectural test reaffirmation

- [x] 6.1 Re-run the existing `NoStaticOrThreadLocalStateTest` on the new `io.flowpipe.observability` package and on the changes to `io.flowpipe.engine` to confirm no static mutable fields or `ThreadLocal`s were introduced (no code change; just verify the test still passes after this slice)

## 7. Documentation

- [x] 7.1 Update `CLAUDE.md` "Module and package layout" to list `io.flowpipe.observability` and its members (`MetricsRecorder`, `NoOpMetricsRecorder`, `StepOutcome`)
- [x] 7.2 Extend the `README.md` example to show plugging in a `MetricsRecorder` (e.g., a Micrometer-style sketch using the SPI) and mention that `step.start` / `step.finish` / `step.error` logs are automatic
- [x] 7.3 Verify the full build is green: `./gradlew clean build` from a fresh checkout
