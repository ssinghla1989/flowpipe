package io.flowpipe.engine;

import io.flowpipe.api.Failure;
import io.flowpipe.api.Result;
import io.flowpipe.api.RetryPolicy;
import io.flowpipe.api.Step;
import io.flowpipe.api.StepContext;
import io.flowpipe.api.StepDescriptor;
import io.flowpipe.api.StepTimeoutException;
import io.flowpipe.api.Success;
import io.flowpipe.api.TimeoutPolicy;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class StepTimeoutExecutionTest {

    // ── 6.1 Step completing within timeout succeeds ───────────────────────────

    @Test
    void step_completing_within_timeout_returns_success() {
        Step<Integer, Integer> fast = stepWithTimeout("fast", TimeoutPolicy.ofMillis(500), (in, ctx) -> in * 2);

        Pipeline<Integer, Integer> pipeline = PipelineBuilder.start(Integer.class).then(fast).build();
        Result<Integer> result = pipeline.execute(5);

        assertThat(result).isInstanceOf(Success.class);
        assertThat(((Success<Integer>) result).value()).isEqualTo(10);
    }

    // ── 6.2 Step exceeding timeout → Failure with StepTimeoutException ────────

    @Test
    void step_exceeding_timeout_returns_failure_with_step_timeout_exception() throws Exception {
        Step<Integer, Integer> slow = stepWithTimeout("slow-step", TimeoutPolicy.ofMillis(50), (in, ctx) -> {
            Thread.sleep(500);
            return in;
        });

        Pipeline<Integer, Integer> pipeline = PipelineBuilder.start(Integer.class).then(slow).build();
        Result<Integer> result = pipeline.execute(1);

        assertThat(result).isInstanceOf(Failure.class);
        Failure<Integer> failure = (Failure<Integer>) result;
        assertThat(failure.failedStepId()).isEqualTo("slow-step");
        assertThat(failure.cause()).isInstanceOf(StepTimeoutException.class);
        StepTimeoutException ex = (StepTimeoutException) failure.cause();
        assertThat(ex.stepId()).isEqualTo("slow-step");
        assertThat(ex.timeoutMs()).isEqualTo(50L);
    }

    // ── 6.3 Downstream step not invoked after timeout failure ─────────────────

    @Test
    void downstream_step_not_invoked_after_timeout() throws Exception {
        AtomicInteger downstreamCalls = new AtomicInteger();

        Step<Integer, Integer> slow = stepWithTimeout("slow", TimeoutPolicy.ofMillis(50), (in, ctx) -> {
            Thread.sleep(500);
            return in;
        });
        Step<Integer, Integer> downstream = stepWithTimeout("downstream", TimeoutPolicy.none(),
            (in, ctx) -> { downstreamCalls.incrementAndGet(); return in; });

        Pipeline<Integer, Integer> pipeline = PipelineBuilder.start(Integer.class)
            .then(slow).then(downstream).build();
        Result<Integer> result = pipeline.execute(1);

        assertThat(result).isInstanceOf(Failure.class);
        assertThat(downstreamCalls.get()).isEqualTo(0);
    }

    // ── 6.4 StepTimeoutException message contains step id and timeout ──────────

    @Test
    void step_timeout_exception_message_contains_step_id_and_timeout() {
        StepTimeoutException ex = new StepTimeoutException("my-step", 250L);
        assertThat(ex.getMessage()).contains("my-step");
        assertThat(ex.getMessage()).contains("250");
    }

    // ── 7.1 Retry + timeout: first attempt times out, second succeeds ─────────

    @Test
    void retry_with_timeout_first_attempt_times_out_second_succeeds() throws Exception {
        AtomicInteger calls = new AtomicInteger();

        Step<Integer, Integer> step = stepWithRetryAndTimeout("timed-retried",
            RetryPolicy.fixed(2, 0), TimeoutPolicy.ofMillis(50), (in, ctx) -> {
                if (calls.incrementAndGet() == 1) {
                    Thread.sleep(500);
                }
                return in * 2;
            });

        Pipeline<Integer, Integer> pipeline = PipelineBuilder.start(Integer.class).then(step).build();
        Result<Integer> result = pipeline.execute(5);

        assertThat(result).isInstanceOf(Success.class);
        assertThat(((Success<Integer>) result).value()).isEqualTo(10);
        assertThat(calls.get()).isEqualTo(2);
    }

    // ── 7.2 Retry + timeout: all attempts time out → Failure ──────────────────

    @Test
    void retry_with_timeout_all_attempts_time_out_returns_failure() throws Exception {
        AtomicInteger calls = new AtomicInteger();

        Step<Integer, Integer> step = stepWithRetryAndTimeout("always-slow",
            RetryPolicy.fixed(3, 0), TimeoutPolicy.ofMillis(50), (in, ctx) -> {
                calls.incrementAndGet();
                Thread.sleep(500);
                return in;
            });

        Pipeline<Integer, Integer> pipeline = PipelineBuilder.start(Integer.class).then(step).build();
        Result<Integer> result = pipeline.execute(1);

        assertThat(result).isInstanceOf(Failure.class);
        Failure<Integer> failure = (Failure<Integer>) result;
        assertThat(failure.cause()).isInstanceOf(StepTimeoutException.class);
        assertThat(calls.get()).isEqualTo(3);
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private interface IntStepBody {
        Integer apply(Integer input, StepContext ctx) throws Exception;
    }

    private static Step<Integer, Integer> stepWithTimeout(String id, TimeoutPolicy timeout, IntStepBody body) {
        StepDescriptor<Integer, Integer> desc = StepDescriptor
            .builder(id, Integer.class, Integer.class)
            .withTimeout(timeout)
            .build();
        return new Step<>() {
            @Override public StepDescriptor<Integer, Integer> describe() { return desc; }
            @Override public Integer execute(Integer input, StepContext ctx) throws Exception {
                return body.apply(input, ctx);
            }
        };
    }

    private static Step<Integer, Integer> stepWithRetryAndTimeout(String id, RetryPolicy retry,
                                                                   TimeoutPolicy timeout, IntStepBody body) {
        StepDescriptor<Integer, Integer> desc = StepDescriptor
            .builder(id, Integer.class, Integer.class)
            .withRetry(retry)
            .withTimeout(timeout)
            .build();
        return new Step<>() {
            @Override public StepDescriptor<Integer, Integer> describe() { return desc; }
            @Override public Integer execute(Integer input, StepContext ctx) throws Exception {
                return body.apply(input, ctx);
            }
        };
    }
}
