package io.flowpipe.engine;

import io.flowpipe.api.CircuitBreakerOpenException;
import io.flowpipe.api.CircuitBreakerPolicy;
import io.flowpipe.api.Failure;
import io.flowpipe.api.Result;
import io.flowpipe.api.RetryPolicy;
import io.flowpipe.api.Step;
import io.flowpipe.api.StepContext;
import io.flowpipe.api.StepDescriptor;
import io.flowpipe.api.Success;
import io.flowpipe.api.TimeoutPolicy;
import io.flowpipe.observability.Slf4jTestAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class CircuitBreakerExecutionTest {

    private Slf4jTestAppender appender;

    @BeforeEach
    void attach() {
        appender = Slf4jTestAppender.attachToEngine();
    }

    @AfterEach
    void detach() {
        appender.detach();
    }

    // ── 4.4 CLOSED → OPEN transition ─────────────────────────────────────────

    @Test
    void circuit_opens_after_failure_rate_threshold_is_reached() {
        // 2 minimumCalls, 100% failure rate >= 50% threshold → should open
        CircuitBreakerPolicy cbp = CircuitBreakerPolicy.of(50, 2, 4, 60_000L, 1);
        Step<Integer, Integer> broken = alwaysFailingStep("s", cbp);
        Pipeline<Integer, Integer> pipeline = PipelineBuilder.start(Integer.class).then(broken).build();

        // Two failures fill the minimum; circuit should now be OPEN
        pipeline.execute(1);
        pipeline.execute(1);

        Result<Integer> result = pipeline.execute(1);

        assertThat(result).isInstanceOf(Failure.class);
        Failure<Integer> failure = (Failure<Integer>) result;
        assertThat(failure.cause()).isInstanceOf(CircuitBreakerOpenException.class);
    }

    @Test
    void circuit_stays_closed_when_failure_rate_is_below_threshold() {
        // 51% threshold; alternating success/failure = 50% rate → should stay CLOSED
        CircuitBreakerPolicy cbp = CircuitBreakerPolicy.of(51, 4, 4, 60_000L, 1);
        AtomicInteger calls = new AtomicInteger();
        Step<Integer, Integer> alternating = stepWithCb("s", cbp, (input, ctx) -> {
            if (calls.incrementAndGet() % 2 == 0) throw new RuntimeException("fail");
            return input;
        });
        Pipeline<Integer, Integer> pipeline = PipelineBuilder.start(Integer.class).then(alternating).build();

        // 4 alternating calls: success, fail, success, fail → 50% failure rate < 51% threshold
        pipeline.execute(1); // success
        pipeline.execute(1); // fail
        pipeline.execute(1); // success
        pipeline.execute(1); // fail

        // 5th call should still go through (circuit still CLOSED)
        Result<Integer> result = pipeline.execute(1);
        assertThat(result).isInstanceOf(Success.class);
    }

    @Test
    void circuit_stays_closed_when_minimum_calls_not_reached() {
        CircuitBreakerPolicy cbp = CircuitBreakerPolicy.of(50, 5, 10, 60_000L, 1);
        Step<Integer, Integer> broken = alwaysFailingStep("s", cbp);
        Pipeline<Integer, Integer> pipeline = PipelineBuilder.start(Integer.class).then(broken).build();

        // 4 failures < minimumCalls of 5
        for (int i = 0; i < 4; i++) {
            pipeline.execute(1);
        }

        // 5th call: this is the call that completes minimumCalls; circuit may now open
        // But the NEXT call after that would be the first OPEN fast-fail
        pipeline.execute(1); // 5th failure → threshold evaluation → circuit opens

        Result<Integer> result = pipeline.execute(1); // 6th call → OPEN fast-fail
        assertThat(result).isInstanceOf(Failure.class);
        assertThat(((Failure<Integer>) result).cause()).isInstanceOf(CircuitBreakerOpenException.class);
    }

    // ── 4.5 OPEN circuit fast-fails without calling execute ──────────────────

    @Test
    void open_circuit_does_not_call_execute_and_returns_failure_with_step_id() {
        CircuitBreakerPolicy cbp = CircuitBreakerPolicy.of(50, 2, 4, 60_000L, 1);
        AtomicInteger executeCalls = new AtomicInteger();
        Step<Integer, Integer> s = stepWithCb("my-step", cbp, (input, ctx) -> {
            if (executeCalls.incrementAndGet() <= 2) throw new RuntimeException("fail");
            return input;
        });
        Pipeline<Integer, Integer> pipeline = PipelineBuilder.start(Integer.class).then(s).build();

        pipeline.execute(1); // fail 1
        pipeline.execute(1); // fail 2 → circuit opens

        int callsBeforeOpen = executeCalls.get();
        Result<Integer> result = pipeline.execute(1); // OPEN → fast-fail

        assertThat(executeCalls.get()).isEqualTo(callsBeforeOpen); // execute NOT called
        assertThat(result).isInstanceOf(Failure.class);
        Failure<Integer> failure = (Failure<Integer>) result;
        assertThat(failure.failedStepId()).isEqualTo("my-step");
        assertThat(failure.cause()).isInstanceOf(CircuitBreakerOpenException.class);
        assertThat(((CircuitBreakerOpenException) failure.cause()).stepId()).isEqualTo("my-step");
    }

    // ── 4.6 OPEN → HALF-OPEN → CLOSED ────────────────────────────────────────

    @Test
    void successful_probes_close_the_circuit() {
        // openWindowMs=0: the window expires immediately, so any call after opening enters HALF-OPEN
        CircuitBreakerPolicy cbp = CircuitBreakerPolicy.of(50, 2, 4, 0L, 2);
        AtomicInteger calls = new AtomicInteger();
        Step<Integer, Integer> s = stepWithCb("s", cbp, (input, ctx) -> {
            int call = calls.incrementAndGet();
            if (call <= 2) throw new RuntimeException("fail"); // fail to open circuit
            return input * 2; // succeed for probes and subsequent calls
        });
        Pipeline<Integer, Integer> pipeline = PipelineBuilder.start(Integer.class).then(s).build();

        pipeline.execute(1); // fail 1
        pipeline.execute(1); // fail 2 → circuit opens

        // openWindowMs=0 → next calls are probes in HALF-OPEN
        Result<Integer> probe1 = pipeline.execute(3); // probe 1 → success
        Result<Integer> probe2 = pipeline.execute(4); // probe 2 → success → closes circuit

        assertThat(probe1).isInstanceOf(Success.class);
        assertThat(probe2).isInstanceOf(Success.class);

        // Circuit is now CLOSED; normal call proceeds
        Result<Integer> afterClose = pipeline.execute(5);
        assertThat(afterClose).isInstanceOf(Success.class);
        assertThat(((Success<Integer>) afterClose).value()).isEqualTo(10);
    }

    // ── 4.7 OPEN → HALF-OPEN → OPEN ──────────────────────────────────────────

    @Test
    void failed_probe_reopens_the_circuit() throws InterruptedException {
        // 100ms open window: we sleep past it to allow probing, then immediately assert
        // the circuit re-opens so the very next call (well within 100ms) is fast-failed.
        CircuitBreakerPolicy cbp = CircuitBreakerPolicy.of(50, 2, 4, 100L, 1);
        AtomicInteger calls = new AtomicInteger();
        Step<Integer, Integer> s = stepWithCb("s", cbp, (input, ctx) -> {
            calls.incrementAndGet();
            throw new RuntimeException("always fails");
        });
        Pipeline<Integer, Integer> pipeline = PipelineBuilder.start(Integer.class).then(s).build();

        pipeline.execute(1); // fail 1
        pipeline.execute(1); // fail 2 → circuit opens

        // Wait for open window to expire so the next call enters HALF-OPEN as a probe
        Thread.sleep(150);

        Result<Integer> probe = pipeline.execute(1); // probe fails → circuit re-opens with fresh timestamp
        assertThat(probe).isInstanceOf(Failure.class);
        assertThat(((Failure<Integer>) probe).cause()).isNotInstanceOf(CircuitBreakerOpenException.class);

        // Next call is immediate (well within 100ms of the re-open) → OPEN fast-fail
        int callsBeforeReopen = calls.get();
        Result<Integer> afterReopen = pipeline.execute(1);
        assertThat(afterReopen).isInstanceOf(Failure.class);
        assertThat(((Failure<Integer>) afterReopen).cause()).isInstanceOf(CircuitBreakerOpenException.class);
        assertThat(calls.get()).isEqualTo(callsBeforeReopen); // execute not called
    }

    // ── 4.8 Retry + circuit breaker: final outcome recorded ──────────────────

    @Test
    void transient_failure_recovered_by_retry_counts_as_one_circuit_success() {
        // Circuit opens at 100% failure rate over minimumCalls=5
        // Step succeeds on 2nd attempt → final outcome = success → circuit should NOT open
        CircuitBreakerPolicy cbp = CircuitBreakerPolicy.of(100, 5, 5, 60_000L, 1);
        AtomicInteger executeCalls = new AtomicInteger();
        Step<Integer, Integer> flaky = stepWithCbAndRetry("s", cbp, RetryPolicy.fixed(3, 0),
            (input, ctx) -> {
                if (executeCalls.incrementAndGet() % 2 != 0) throw new RuntimeException("transient");
                return input;
            });
        Pipeline<Integer, Integer> pipeline = PipelineBuilder.start(Integer.class).then(flaky).build();

        // 5 pipeline executions, each recovering on 2nd attempt → all succeed at circuit level
        for (int i = 0; i < 5; i++) {
            Result<Integer> r = pipeline.execute(1);
            assertThat(r).isInstanceOf(Success.class);
        }

        // Circuit should still be CLOSED (0 failures recorded from circuit's perspective)
        Result<Integer> result = pipeline.execute(1);
        assertThat(result).isInstanceOf(Success.class);
        assertThat(result).isNotInstanceOf(Failure.class);
    }

    @Test
    void exhausted_retries_count_as_one_circuit_failure() {
        // minimumCalls=2; each pipeline call fails all retries → 2 calls should open circuit
        CircuitBreakerPolicy cbp = CircuitBreakerPolicy.of(50, 2, 4, 60_000L, 1);
        Step<Integer, Integer> broken = stepWithCbAndRetry("s", cbp, RetryPolicy.fixed(3, 0),
            (input, ctx) -> { throw new RuntimeException("always"); });
        Pipeline<Integer, Integer> pipeline = PipelineBuilder.start(Integer.class).then(broken).build();

        pipeline.execute(1); // retry 3 times, all fail → 1 failure recorded to circuit
        pipeline.execute(1); // retry 3 times, all fail → 2nd failure → circuit opens

        Result<Integer> result = pipeline.execute(1);
        assertThat(result).isInstanceOf(Failure.class);
        assertThat(((Failure<Integer>) result).cause()).isInstanceOf(CircuitBreakerOpenException.class);
    }

    // ── 4.9 Per-instance isolation ────────────────────────────────────────────

    @Test
    void circuit_state_is_isolated_between_pipeline_instances() {
        CircuitBreakerPolicy cbp = CircuitBreakerPolicy.of(50, 2, 4, 60_000L, 1);
        Step<Integer, Integer> broken1 = alwaysFailingStep("s", cbp);

        Pipeline<Integer, Integer> pipeline1 = PipelineBuilder.start(Integer.class).then(broken1).build();

        // Open circuit on pipeline1
        pipeline1.execute(1);
        pipeline1.execute(1); // circuit opens on pipeline1

        // Build a completely separate pipeline instance; its circuit starts CLOSED
        Pipeline<Integer, Integer> freshPipeline2 = PipelineBuilder.start(Integer.class)
            .then(stepWithCb("s", cbp, (input, ctx) -> input * 3))
            .build();
        Result<Integer> result = freshPipeline2.execute(5);
        assertThat(result).isInstanceOf(Success.class);
        assertThat(((Success<Integer>) result).value()).isEqualTo(15);
    }

    // ── 4.10 Circuit breaker + timeout: OPEN short-circuits before timeout ────

    @Test
    void open_circuit_short_circuits_before_timeout_enforcement() {
        CircuitBreakerPolicy cbp = CircuitBreakerPolicy.of(50, 2, 4, 60_000L, 1);
        AtomicInteger executeCalls = new AtomicInteger();

        // Step that would sleep forever if execute() were called
        StepDescriptor<Integer, Integer> desc = StepDescriptor.builder("s", Integer.class, Integer.class)
            .withCircuitBreaker(cbp)
            .withTimeout(TimeoutPolicy.ofMillis(100))
            .build();
        Step<Integer, Integer> slowStep = new Step<>() {
            @Override public StepDescriptor<Integer, Integer> describe() { return desc; }
            @Override public Integer execute(Integer input, StepContext ctx) throws Exception {
                executeCalls.incrementAndGet();
                throw new RuntimeException("always fails");
            }
        };

        Pipeline<Integer, Integer> pipeline = PipelineBuilder.start(Integer.class).then(slowStep).build();

        pipeline.execute(1); // fail 1
        pipeline.execute(1); // fail 2 → circuit opens

        int callsBefore = executeCalls.get();
        long start = System.currentTimeMillis();
        Result<Integer> result = pipeline.execute(1); // OPEN → fast-fail immediately
        long elapsed = System.currentTimeMillis() - start;

        assertThat(executeCalls.get()).isEqualTo(callsBefore); // execute not called
        assertThat(result).isInstanceOf(Failure.class);
        assertThat(((Failure<Integer>) result).cause()).isInstanceOf(CircuitBreakerOpenException.class);
        assertThat(elapsed).isLessThan(100); // did not wait for the timeout
    }

    // ── 5.1 step.circuit_open log event ──────────────────────────────────────

    @Test
    void open_circuit_emits_circuit_open_log_event_and_not_step_start() {
        CircuitBreakerPolicy cbp = CircuitBreakerPolicy.of(50, 2, 4, 60_000L, 1);
        Step<Integer, Integer> broken = alwaysFailingStep("log-step", cbp);
        Pipeline<Integer, Integer> pipeline = PipelineBuilder.start(Integer.class).then(broken).build();

        pipeline.execute(1); // fail 1
        pipeline.execute(1); // fail 2 → circuit opens
        appender.clear();

        pipeline.execute(1); // OPEN fast-fail

        assertThat(appender.events("step.circuit_open")).hasSize(1);
        var fields = Slf4jTestAppender.fields(appender.events("step.circuit_open").get(0));
        assertThat(fields).containsKey("step.id");
        assertThat(fields).containsKey("step.retriable_after");

        // step.start must NOT be emitted for the open fast-fail call
        assertThat(appender.events("step.start")).isEmpty();
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    @FunctionalInterface
    private interface StepBody {
        Integer apply(Integer input, StepContext ctx) throws Exception;
    }

    private static Step<Integer, Integer> alwaysFailingStep(String id, CircuitBreakerPolicy cbp) {
        return stepWithCb(id, cbp, (input, ctx) -> { throw new RuntimeException("always fails"); });
    }

    private static Step<Integer, Integer> stepWithCb(String id, CircuitBreakerPolicy cbp, StepBody body) {
        StepDescriptor<Integer, Integer> desc = StepDescriptor.builder(id, Integer.class, Integer.class)
            .withCircuitBreaker(cbp)
            .build();
        return new Step<>() {
            @Override public StepDescriptor<Integer, Integer> describe() { return desc; }
            @Override public Integer execute(Integer input, StepContext ctx) throws Exception {
                return body.apply(input, ctx);
            }
        };
    }

    private static Step<Integer, Integer> stepWithCbAndRetry(String id, CircuitBreakerPolicy cbp,
                                                              RetryPolicy rp, StepBody body) {
        StepDescriptor<Integer, Integer> desc = StepDescriptor.builder(id, Integer.class, Integer.class)
            .withCircuitBreaker(cbp)
            .withRetry(rp)
            .build();
        return new Step<>() {
            @Override public StepDescriptor<Integer, Integer> describe() { return desc; }
            @Override public Integer execute(Integer input, StepContext ctx) throws Exception {
                return body.apply(input, ctx);
            }
        };
    }
}
