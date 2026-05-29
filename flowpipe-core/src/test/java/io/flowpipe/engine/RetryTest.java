package io.flowpipe.engine;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import io.flowpipe.api.Failure;
import io.flowpipe.api.Result;
import io.flowpipe.api.RetryPolicy;
import io.flowpipe.api.Step;
import io.flowpipe.api.StepContext;
import io.flowpipe.api.StepDescriptor;
import io.flowpipe.api.Success;
import io.flowpipe.observability.Slf4jTestAppender;
import io.flowpipe.observability.TestMetricsRecorder;
import io.flowpipe.observability.TestMetricsRecorder.AttemptsEvent;
import io.flowpipe.observability.TestMetricsRecorder.RetryAttemptEvent;
import io.flowpipe.validation.ValidationException;
import io.flowpipe.validation.Validator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RetryTest {

    private Slf4jTestAppender appender;

    @BeforeEach
    void attach() {
        appender = Slf4jTestAppender.attachToEngine();
    }

    @AfterEach
    void detach() {
        appender.detach();
    }

    // ── 6.1 RetryPolicy factory validation ───────────────────────────────────

    @Test
    void retry_policy_rejects_max_attempts_less_than_one() {
        assertThatThrownBy(() -> RetryPolicy.fixed(0, 100))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> RetryPolicy.exponential(0, 100, 30_000, 2.0, false))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void retry_policy_rejects_negative_initial_delay() {
        assertThatThrownBy(() -> RetryPolicy.fixed(2, -1))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> RetryPolicy.exponential(2, -1, 30_000, 2.0, false))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void retry_policy_rejects_multiplier_below_one() {
        assertThatThrownBy(() -> RetryPolicy.exponential(2, 100, 30_000, 0.9, false))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void retry_policy_exponential_rejects_zero_initial_delay() {
        assertThatThrownBy(() -> RetryPolicy.exponential(2, 0, 30_000, 2.0, false))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("use RetryPolicy.fixed()");
    }

    @Test
    void retry_policy_exponential_rejects_max_delay_below_initial_delay() {
        assertThatThrownBy(() -> RetryPolicy.exponential(2, 500, 100, 2.0, false))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("maxDelayMs");
    }

    @Test
    void retry_policy_none_has_max_attempts_one_and_zero_delay() {
        RetryPolicy p = RetryPolicy.none();
        assertThat(p.maxAttempts()).isEqualTo(1);
        assertThat(p.initialDelayMs()).isEqualTo(0L);
        assertThat(p.multiplier()).isEqualTo(1.0);
        assertThat(p.jitter()).isFalse();
    }

    @Test
    void retry_policy_fixed_sets_max_attempts_and_delay() {
        RetryPolicy p = RetryPolicy.fixed(3, 500);
        assertThat(p.maxAttempts()).isEqualTo(3);
        assertThat(p.initialDelayMs()).isEqualTo(500L);
    }

    // ── 6.2 Step succeeds on second attempt ──────────────────────────────────

    @Test
    void step_succeeds_on_second_attempt_execute_called_twice() {
        AtomicInteger calls = new AtomicInteger();
        Step<Integer, Integer> flaky = stepWithRetry("flaky", RetryPolicy.fixed(3, 0), (input, ctx) -> {
            if (calls.incrementAndGet() < 2) throw new RuntimeException("transient");
            return input * 2;
        });

        Pipeline<Integer, Integer> pipeline = PipelineBuilder.start(Integer.class).then(flaky).build();
        Result<Integer> result = pipeline.execute(5);

        assertThat(result).isInstanceOf(Success.class);
        assertThat(((Success<Integer>) result).value()).isEqualTo(10);
        assertThat(calls.get()).isEqualTo(2);
    }

    // ── 6.3 Step exhausts all attempts → Failure ─────────────────────────────

    @Test
    void step_exhausts_all_attempts_and_pipeline_returns_failure() {
        AtomicInteger calls = new AtomicInteger();
        RuntimeException boom = new RuntimeException("always fails");
        Step<Integer, Integer> broken = stepWithRetry("broken", RetryPolicy.fixed(3, 0), (input, ctx) -> {
            calls.incrementAndGet();
            throw boom;
        });

        Pipeline<Integer, Integer> pipeline = PipelineBuilder.start(Integer.class).then(broken).build();
        Result<Integer> result = pipeline.execute(1);

        assertThat(result).isInstanceOf(Failure.class);
        Failure<Integer> failure = (Failure<Integer>) result;
        assertThat(failure.failedStepId()).isEqualTo("broken");
        assertThat(failure.cause()).isSameAs(boom);
        assertThat(calls.get()).isEqualTo(3);
    }

    // ── 6.4 RetryPolicy.none() means a single attempt, no retry ──────────────

    @Test
    void retry_policy_none_produces_single_attempt_and_failure() {
        AtomicInteger calls = new AtomicInteger();
        Step<Integer, Integer> s = stepWithRetry("s", RetryPolicy.none(), (input, ctx) -> {
            calls.incrementAndGet();
            throw new RuntimeException("fail");
        });

        Pipeline<Integer, Integer> pipeline = PipelineBuilder.start(Integer.class).then(s).build();
        Result<Integer> result = pipeline.execute(1);

        assertThat(result).isInstanceOf(Failure.class);
        assertThat(calls.get()).isEqualTo(1);
        assertThat(appender.events("step.retry")).isEmpty();
    }

    // ── 6.5 Input validation runs on every attempt ───────────────────────────

    @Test
    void input_validation_runs_on_every_attempt() {
        AtomicInteger validationCalls = new AtomicInteger();
        AtomicInteger executeCalls = new AtomicInteger();

        Validator<Integer> countingValidator = input -> validationCalls.incrementAndGet();

        Step<Integer, Integer> s = new Step<>() {
            private final StepDescriptor<Integer, Integer> desc = StepDescriptor
                .builder("s", Integer.class, Integer.class)
                .inputValidator(countingValidator)
                .withRetry(RetryPolicy.fixed(3, 0))
                .build();

            @Override
            public StepDescriptor<Integer, Integer> describe() { return desc; }

            @Override
            public Integer execute(Integer input, StepContext ctx) {
                if (executeCalls.incrementAndGet() < 3) throw new RuntimeException("retry me");
                return input;
            }
        };

        Pipeline<Integer, Integer> pipeline = PipelineBuilder.start(Integer.class).then(s).build();
        pipeline.execute(1);

        assertThat(validationCalls.get()).isEqualTo(3);
    }

    // ── 6.6 Output validation failure triggers retry ─────────────────────────

    @Test
    void output_validation_failure_triggers_retry() {
        AtomicInteger executeCalls = new AtomicInteger();
        AtomicInteger outputValidationCalls = new AtomicInteger();

        Validator<Integer> rejectFirstOutput = value -> {
            if (outputValidationCalls.incrementAndGet() == 1) {
                throw new ValidationException("bad output on first attempt");
            }
        };

        Step<Integer, Integer> s = new Step<>() {
            private final StepDescriptor<Integer, Integer> desc = StepDescriptor
                .builder("s", Integer.class, Integer.class)
                .outputValidator(rejectFirstOutput)
                .withRetry(RetryPolicy.fixed(2, 0))
                .build();

            @Override
            public StepDescriptor<Integer, Integer> describe() { return desc; }

            @Override
            public Integer execute(Integer input, StepContext ctx) {
                executeCalls.incrementAndGet();
                return input;
            }
        };

        Pipeline<Integer, Integer> pipeline = PipelineBuilder.start(Integer.class).then(s).build();
        Result<Integer> result = pipeline.execute(42);

        assertThat(result).isInstanceOf(Success.class);
        assertThat(executeCalls.get()).isEqualTo(2);
    }

    // ── 6.7 Downstream steps not invoked after retry exhaustion ──────────────

    @Test
    void downstream_steps_not_invoked_after_retry_exhaustion() {
        AtomicInteger downstreamCalls = new AtomicInteger();

        Step<Integer, Integer> failing = stepWithRetry("fail", RetryPolicy.fixed(2, 0),
            (input, ctx) -> { throw new RuntimeException("boom"); });
        Step<Integer, Integer> downstream = stepWithRetry("downstream", RetryPolicy.none(),
            (input, ctx) -> { downstreamCalls.incrementAndGet(); return input; });

        Pipeline<Integer, Integer> pipeline = PipelineBuilder.start(Integer.class)
            .then(failing).then(downstream).build();
        Result<Integer> result = pipeline.execute(1);

        assertThat(result).isInstanceOf(Failure.class);
        assertThat(((Failure<Integer>) result).failedStepId()).isEqualTo("fail");
        assertThat(downstreamCalls.get()).isEqualTo(0);
    }

    // ── 6.8 step.retry log emitted per non-final failed attempt ──────────────

    @Test
    void step_retry_log_emitted_for_each_non_final_failed_attempt() {
        AtomicInteger calls = new AtomicInteger();
        Step<Integer, Integer> flaky = stepWithRetry("flaky", RetryPolicy.fixed(3, 100), (input, ctx) -> {
            if (calls.incrementAndGet() < 3) throw new RuntimeException("transient");
            return input;
        });

        Pipeline<Integer, Integer> pipeline = PipelineBuilder.start(Integer.class).then(flaky).build();
        pipeline.execute(1);

        List<ILoggingEvent> retryEvents = appender.events("step.retry");
        assertThat(retryEvents).hasSize(2);

        Map<String, Object> first = Slf4jTestAppender.fields(retryEvents.get(0));
        assertThat(first)
            .containsEntry("step.id", "flaky")
            .containsEntry("step.attempt", 1)
            .containsEntry("step.max_attempts", 3);

        Map<String, Object> second = Slf4jTestAppender.fields(retryEvents.get(1));
        assertThat(second)
            .containsEntry("step.id", "flaky")
            .containsEntry("step.attempt", 2)
            .containsEntry("step.max_attempts", 3);

        assertThat(retryEvents.get(0).getLevel()).isEqualTo(Level.WARN);
        assertThat(retryEvents.get(1).getLevel()).isEqualTo(Level.WARN);
    }

    @Test
    void no_step_retry_log_on_final_failed_attempt() {
        AtomicInteger calls = new AtomicInteger();
        Step<Integer, Integer> s = stepWithRetry("s", RetryPolicy.fixed(2, 0), (input, ctx) -> {
            calls.incrementAndGet();
            throw new RuntimeException("fail");
        });

        Pipeline<Integer, Integer> pipeline = PipelineBuilder.start(Integer.class).then(s).build();
        pipeline.execute(1);

        // Only one retry event: after attempt 1, not after attempt 2 (the final failure)
        assertThat(appender.events("step.retry")).hasSize(1);
        assertThat(Slf4jTestAppender.fields(appender.events("step.retry").get(0)))
            .containsEntry("step.attempt", 1);
    }

    // ── 6.9 No step.retry event when RetryPolicy.none() ─────────────────────

    @Test
    void no_step_retry_event_when_retry_policy_none() {
        Step<Integer, Integer> s = stepWithRetry("s", RetryPolicy.none(),
            (input, ctx) -> { throw new RuntimeException("fail"); });

        Pipeline<Integer, Integer> pipeline = PipelineBuilder.start(Integer.class).then(s).build();
        pipeline.execute(1);

        assertThat(appender.events("step.retry")).isEmpty();
    }

    // ── 6.10 recordRetryAttempt called per non-final failed attempt ───────────

    @Test
    void record_retry_attempt_called_once_per_non_final_failed_attempt() {
        TestMetricsRecorder rec = new TestMetricsRecorder();
        AtomicInteger calls = new AtomicInteger();
        Step<Integer, Integer> unstable = stepWithRetry("unstable", RetryPolicy.fixed(3, 0), (input, ctx) -> {
            if (calls.incrementAndGet() < 3) throw new RuntimeException("transient");
            return input;
        });

        Pipeline<Integer, Integer> pipeline = PipelineBuilder.start(Integer.class)
            .then(unstable).withMetrics(rec).build();
        pipeline.execute(1);

        List<TestMetricsRecorder.Event> retries = rec.events("unstable").stream()
            .filter(e -> e instanceof RetryAttemptEvent).toList();
        assertThat(retries).hasSize(2);
        assertThat(((RetryAttemptEvent) retries.get(0)).attemptNumber()).isEqualTo(1);
        assertThat(((RetryAttemptEvent) retries.get(1)).attemptNumber()).isEqualTo(2);
    }

    @Test
    void record_retry_attempt_not_called_on_final_failure() {
        TestMetricsRecorder rec = new TestMetricsRecorder();
        Step<Integer, Integer> broken = stepWithRetry("broken", RetryPolicy.fixed(2, 0),
            (input, ctx) -> { throw new RuntimeException("always"); });

        Pipeline<Integer, Integer> pipeline = PipelineBuilder.start(Integer.class)
            .then(broken).withMetrics(rec).build();
        pipeline.execute(1);

        List<TestMetricsRecorder.Event> retries = rec.events("broken").stream()
            .filter(e -> e instanceof RetryAttemptEvent).toList();
        // Only one: after attempt 1. Not after attempt 2 (the final failure).
        assertThat(retries).hasSize(1);
        assertThat(((RetryAttemptEvent) retries.get(0)).attemptNumber()).isEqualTo(1);
    }

    @Test
    void record_retry_attempt_not_called_when_policy_none() {
        TestMetricsRecorder rec = new TestMetricsRecorder();
        Step<Integer, Integer> s = stepWithRetry("s", RetryPolicy.none(),
            (input, ctx) -> { throw new RuntimeException("fail"); });

        Pipeline<Integer, Integer> pipeline = PipelineBuilder.start(Integer.class)
            .then(s).withMetrics(rec).build();
        pipeline.execute(1);

        assertThat(rec.events("s").stream().filter(e -> e instanceof RetryAttemptEvent).toList()).isEmpty();
    }

    // ── 6.11 recordStepAttempts receives actual attempt count ─────────────────

    @Test
    void record_step_attempts_reflects_actual_attempt_count_on_success_after_retry() {
        TestMetricsRecorder rec = new TestMetricsRecorder();
        AtomicInteger calls = new AtomicInteger();
        Step<Integer, Integer> flaky = stepWithRetry("flaky", RetryPolicy.fixed(3, 0), (input, ctx) -> {
            if (calls.incrementAndGet() < 2) throw new RuntimeException("transient");
            return input;
        });

        PipelineBuilder.start(Integer.class).then(flaky).withMetrics(rec).build().execute(1);

        List<AttemptsEvent> attemptsEvents = rec.events("flaky").stream()
            .filter(e -> e instanceof AttemptsEvent).map(e -> (AttemptsEvent) e).toList();
        assertThat(attemptsEvents).hasSize(1);
        assertThat(attemptsEvents.get(0).attempts()).isEqualTo(2);
    }

    @Test
    void record_step_attempts_reflects_max_when_all_fail() {
        TestMetricsRecorder rec = new TestMetricsRecorder();
        Step<Integer, Integer> broken = stepWithRetry("broken", RetryPolicy.fixed(3, 0),
            (input, ctx) -> { throw new RuntimeException("always"); });

        PipelineBuilder.start(Integer.class).then(broken).withMetrics(rec).build().execute(1);

        List<AttemptsEvent> attemptsEvents = rec.events("broken").stream()
            .filter(e -> e instanceof AttemptsEvent).map(e -> (AttemptsEvent) e).toList();
        assertThat(attemptsEvents).hasSize(1);
        assertThat(attemptsEvents.get(0).attempts()).isEqualTo(3);
    }

    // ── 6.12 recordStepAttempts = 1 for steps succeeding on first attempt ────

    @Test
    void record_step_attempts_is_one_for_steps_succeeding_on_first_attempt() {
        TestMetricsRecorder rec = new TestMetricsRecorder();
        Step<Integer, Integer> s = Step.of("s", Integer.class, Integer.class, (input, ctx) -> input);

        PipelineBuilder.start(Integer.class).then(s).withMetrics(rec).build().execute(1);

        List<AttemptsEvent> attemptsEvents = rec.events("s").stream()
            .filter(e -> e instanceof AttemptsEvent).map(e -> (AttemptsEvent) e).toList();
        assertThat(attemptsEvents).hasSize(1);
        assertThat(attemptsEvents.get(0).attempts()).isEqualTo(1);
    }

    // ── 6.13 Exponential delay computation ────────────────────────────────────

    @Test
    void exponential_delay_values_are_reported_correctly_in_retry_log_events() {
        AtomicInteger calls = new AtomicInteger();
        // 4 attempts, 100ms initial delay, 2x multiplier — always fails so we get 3 retries
        Step<Integer, Integer> s = stepWithRetry("s",
            RetryPolicy.exponential(4, 100, 30_000, 2.0, false),
            (input, ctx) -> { calls.incrementAndGet(); throw new RuntimeException("fail"); });

        Pipeline<Integer, Integer> pipeline = PipelineBuilder.start(Integer.class).then(s).build();
        pipeline.execute(1);

        List<ILoggingEvent> retryEvents = appender.events("step.retry");
        assertThat(retryEvents).hasSize(3);

        // delay before attempt 2: floor(100 * 2.0^0) = 100
        assertThat(Slf4jTestAppender.fields(retryEvents.get(0))).containsEntry("step.delay_ms", 100L);
        // delay before attempt 3: floor(100 * 2.0^1) = 200
        assertThat(Slf4jTestAppender.fields(retryEvents.get(1))).containsEntry("step.delay_ms", 200L);
        // delay before attempt 4: floor(100 * 2.0^2) = 400
        assertThat(Slf4jTestAppender.fields(retryEvents.get(2))).containsEntry("step.delay_ms", 400L);
    }

    // ── helper ────────────────────────────────────────────────────────────────

    @FunctionalInterface
    private interface IntStepBody {
        Integer apply(Integer input, StepContext ctx) throws Exception;
    }

    private static Step<Integer, Integer> stepWithRetry(String id, RetryPolicy policy, IntStepBody body) {
        StepDescriptor<Integer, Integer> desc = StepDescriptor
            .builder(id, Integer.class, Integer.class)
            .withRetry(policy)
            .build();
        return new Step<>() {
            @Override public StepDescriptor<Integer, Integer> describe() { return desc; }
            @Override public Integer execute(Integer input, StepContext ctx) throws Exception {
                return body.apply(input, ctx);
            }
        };
    }
}
