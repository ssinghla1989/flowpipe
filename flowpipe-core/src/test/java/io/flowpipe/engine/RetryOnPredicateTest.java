package io.flowpipe.engine;

import io.flowpipe.api.Failure;
import io.flowpipe.api.Result;
import io.flowpipe.api.RetryPolicy;
import io.flowpipe.api.Step;
import io.flowpipe.api.StepContext;
import io.flowpipe.api.StepDescriptor;
import io.flowpipe.api.Success;
import io.flowpipe.observability.Slf4jTestAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for RetryPolicy.retryOn(Predicate<Throwable>): selective retry based on exception type.
 */
class RetryOnPredicateTest {

    private Slf4jTestAppender appender;

    @BeforeEach
    void attach() {
        appender = Slf4jTestAppender.attachToEngine();
    }

    @AfterEach
    void detach() {
        appender.detach();
    }

    // -------------------------------------------------------------------------
    // Basic predicate filtering
    // -------------------------------------------------------------------------

    @Test
    void retryOn_predicate_retries_when_predicate_returns_true() {
        AtomicInteger calls = new AtomicInteger();
        RetryPolicy policy = RetryPolicy.fixed(3, 0)
            .retryOn(e -> e instanceof IOException);

        Step<String, String> step = stepWith("io-step", policy, (s, ctx) -> {
            if (calls.incrementAndGet() < 3) throw new IOException("transient");
            return "ok";
        });

        Result<String> result = PipelineBuilder.start(String.class).then(step).build().execute("x");

        assertThat(result).isInstanceOf(Success.class);
        assertThat(((Success<String>) result).value()).isEqualTo("ok");
        assertThat(calls.get()).isEqualTo(3);
    }

    @Test
    void retryOn_predicate_does_not_retry_when_predicate_returns_false() {
        AtomicInteger calls = new AtomicInteger();
        RetryPolicy policy = RetryPolicy.fixed(3, 0)
            .retryOn(e -> e instanceof IOException);  // only retry IOException

        Step<String, String> step = stepWith("bad-step", policy, (s, ctx) -> {
            calls.incrementAndGet();
            throw new IllegalArgumentException("not retried");  // predicate returns false
        });

        Result<String> result = PipelineBuilder.start(String.class).then(step).build().execute("x");

        assertThat(result).isInstanceOf(Failure.class);
        assertThat(((Failure<String>) result).cause()).isInstanceOf(IllegalArgumentException.class);
        assertThat(calls.get()).isEqualTo(1);  // only one attempt — not retried
    }

    @Test
    void retryOn_predicate_non_matching_exception_propagates_as_original_cause() {
        RuntimeException original = new IllegalStateException("not an IOException");
        RetryPolicy policy = RetryPolicy.fixed(3, 0)
            .retryOn(e -> e instanceof IOException);

        Step<String, String> step = stepWith("s", policy, (s, ctx) -> { throw original; });

        Result<String> result = PipelineBuilder.start(String.class).then(step).build().execute("x");

        assertThat(result).isInstanceOf(Failure.class);
        assertThat(((Failure<String>) result).cause()).isSameAs(original);
    }

    // -------------------------------------------------------------------------
    // Mixed: some attempts match predicate, then one doesn't
    // -------------------------------------------------------------------------

    @Test
    void retryOn_stops_retrying_when_non_matching_exception_thrown_on_later_attempt() {
        AtomicInteger calls = new AtomicInteger();
        RetryPolicy policy = RetryPolicy.fixed(5, 0)
            .retryOn(e -> e instanceof IOException);

        Step<String, String> step = stepWith("mixed", policy, (s, ctx) -> {
            int n = calls.incrementAndGet();
            if (n == 1) throw new IOException("first — retried");
            if (n == 2) throw new IOException("second — retried");
            throw new IllegalArgumentException("third — NOT retried");
        });

        Result<String> result = PipelineBuilder.start(String.class).then(step).build().execute("x");

        assertThat(result).isInstanceOf(Failure.class);
        assertThat(((Failure<String>) result).cause()).isInstanceOf(IllegalArgumentException.class);
        assertThat(calls.get()).isEqualTo(3);  // stopped at attempt 3
    }

    @Test
    void retryOn_non_matching_exception_step_error_event_is_emitted() {
        RetryPolicy policy = RetryPolicy.fixed(3, 0)
            .retryOn(e -> e instanceof IOException);

        Step<String, String> step = stepWith("emit-check", policy, (s, ctx) -> {
            throw new IllegalArgumentException("should still emit step.error");
        });

        PipelineBuilder.start(String.class).then(step).build().execute("x");

        // step.error MUST be emitted even though the predicate rejected the exception
        assertThat(appender.events("step.error")).hasSize(1);
        assertThat(Slf4jTestAppender.fields(appender.events("step.error").get(0)))
            .containsEntry("step.id", "emit-check");
    }

    // -------------------------------------------------------------------------
    // Predicate on all attempts failing (exhaustion)
    // -------------------------------------------------------------------------

    @Test
    void retryOn_all_attempts_fail_matching_exception_returns_failure() {
        AtomicInteger calls = new AtomicInteger();
        RetryPolicy policy = RetryPolicy.fixed(3, 0)
            .retryOn(e -> e instanceof IOException);

        Step<String, String> step = stepWith("always-io", policy, (s, ctx) -> {
            calls.incrementAndGet();
            throw new IOException("always fails");
        });

        Result<String> result = PipelineBuilder.start(String.class).then(step).build().execute("x");

        assertThat(result).isInstanceOf(Failure.class);
        assertThat(((Failure<String>) result).cause()).isInstanceOf(IOException.class);
        assertThat(calls.get()).isEqualTo(3);
    }

    @Test
    void retryOn_exhaustion_emits_step_error_for_each_attempt() {
        RetryPolicy policy = RetryPolicy.fixed(3, 0)
            .retryOn(e -> e instanceof IOException);

        Step<String, String> step = stepWith("always-io", policy, (s, ctx) -> {
            throw new IOException("fail");
        });

        PipelineBuilder.start(String.class).then(step).build().execute("x");

        assertThat(appender.events("step.error")).hasSize(3);
    }

    // -------------------------------------------------------------------------
    // Null predicate (default) — retries on any exception
    // -------------------------------------------------------------------------

    @Test
    void null_predicate_retries_on_any_exception() {
        AtomicInteger calls = new AtomicInteger();
        // No .retryOn() call — default behaviour
        RetryPolicy policy = RetryPolicy.fixed(3, 0);

        Step<String, String> step = stepWith("any-ex", policy, (s, ctx) -> {
            if (calls.incrementAndGet() < 3) throw new IllegalStateException("any exception retried");
            return "done";
        });

        Result<String> result = PipelineBuilder.start(String.class).then(step).build().execute("x");

        assertThat(result).isInstanceOf(Success.class);
        assertThat(calls.get()).isEqualTo(3);
    }

    // -------------------------------------------------------------------------
    // RetryPolicy immutability — retryOn returns a new instance
    // -------------------------------------------------------------------------

    @Test
    void retryOn_returns_new_instance_leaving_original_unchanged() {
        RetryPolicy base = RetryPolicy.fixed(3, 0);
        RetryPolicy withPred = base.retryOn(e -> e instanceof IOException);

        assertThat(withPred).isNotSameAs(base);
        assertThat(base.retryPredicate()).isNull();
        assertThat(withPred.retryPredicate()).isNotNull();
        // Core fields are preserved
        assertThat(withPred.maxAttempts()).isEqualTo(3);
        assertThat(withPred.initialDelayMs()).isEqualTo(0L);
    }

    // -------------------------------------------------------------------------
    // Integration: retryOn with exponential backoff
    // -------------------------------------------------------------------------

    @Test
    void retryOn_with_exponential_backoff_retries_matching_and_stops_on_mismatch() {
        AtomicInteger calls = new AtomicInteger();
        RetryPolicy policy = RetryPolicy.exponential(5, 0, 2.0, false)
            .retryOn(e -> e instanceof IOException);

        Step<String, String> step = stepWith("exp", policy, (s, ctx) -> {
            int n = calls.incrementAndGet();
            if (n < 3) throw new IOException("retried");
            throw new RuntimeException("stops here");
        });

        Result<String> result = PipelineBuilder.start(String.class).then(step).build().execute("x");

        assertThat(result).isInstanceOf(Failure.class);
        assertThat(((Failure<String>) result).cause()).isInstanceOf(RuntimeException.class);
        assertThat(calls.get()).isEqualTo(3);
    }

    // -------------------------------------------------------------------------
    // Step.error log correctness for mixed scenario
    // -------------------------------------------------------------------------

    @Test
    void step_error_events_carry_correct_attempt_numbers() {
        AtomicInteger calls = new AtomicInteger();
        RetryPolicy policy = RetryPolicy.fixed(5, 0)
            .retryOn(e -> e instanceof IOException);

        Step<String, String> step = stepWith("counted", policy, (s, ctx) -> {
            int n = calls.incrementAndGet();
            if (n <= 2) throw new IOException("retried attempt " + n);
            throw new IllegalArgumentException("stops at attempt 3");
        });

        PipelineBuilder.start(String.class).then(step).build().execute("x");

        // 3 step.error events: attempt 1, 2 (from onFailedAttempt listener), 3 (from catch block)
        assertThat(appender.events("step.error")).hasSize(3);
        assertThat(Slf4jTestAppender.fields(appender.events("step.error").get(0)))
            .containsEntry("step.attempt", 1);
        assertThat(appender.fields(appender.events("step.error").get(1)))
            .containsEntry("step.attempt", 2);
        assertThat(appender.fields(appender.events("step.error").get(2)))
            .containsEntry("step.attempt", 3);
    }

    // -------------------------------------------------------------------------
    // helper
    // -------------------------------------------------------------------------

    @FunctionalInterface
    private interface StepBody {
        String apply(String input, StepContext ctx) throws Exception;
    }

    private static Step<String, String> stepWith(String id, RetryPolicy policy, StepBody body) {
        StepDescriptor<String, String> desc = StepDescriptor
            .builder(id, String.class, String.class)
            .withRetry(policy)
            .build();
        return new Step<>() {
            @Override public StepDescriptor<String, String> describe() { return desc; }
            @Override public String execute(String input, StepContext ctx) throws Exception {
                return body.apply(input, ctx);
            }
        };
    }
}
