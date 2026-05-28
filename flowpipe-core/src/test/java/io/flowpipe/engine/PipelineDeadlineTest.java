package io.flowpipe.engine;

import io.flowpipe.api.Failure;
import io.flowpipe.api.PipelineDeadlineExceededException;
import io.flowpipe.api.Result;
import io.flowpipe.api.Step;
import io.flowpipe.api.Success;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PipelineDeadlineTest {

    @Test
    void pipeline_within_deadline_succeeds() {
        Step<String, String> fast = Step.of("fast", String.class, String.class, (s, ctx) -> s);

        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
            .then(fast)
            .withDeadline(5_000)
            .build();

        Result<String> result = pipeline.execute("hello");

        assertThat(result).isInstanceOf(Success.class);
        assertThat(((Success<String>) result).value()).isEqualTo("hello");
    }

    @Test
    void deadline_exceeded_between_steps_produces_failure() {
        AtomicBoolean secondStepRan = new AtomicBoolean(false);

        Step<String, String> slow = Step.of("slow", String.class, String.class, (s, ctx) -> {
            sleepUninterruptibly(150);
            return s;
        });
        Step<String, String> after = Step.of("after", String.class, String.class, (s, ctx) -> {
            secondStepRan.set(true);
            return s;
        });

        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
            .then(slow)
            .then(after)
            .withDeadline(20)
            .build();

        Result<String> result = pipeline.execute("x");

        assertThat(result).isInstanceOf(Failure.class);
        Failure<String> failure = (Failure<String>) result;
        assertThat(failure.cause()).isInstanceOf(PipelineDeadlineExceededException.class);
        assertThat(failure.failedStepId()).isEqualTo("pipeline.deadline");
        assertThat(secondStepRan.get()).isFalse();
    }

    @Test
    void deadline_exception_carries_configured_deadline_ms() {
        Step<String, String> slow = Step.of("slow", String.class, String.class, (s, ctx) -> {
            sleepUninterruptibly(200);
            return s;
        });
        Step<String, String> next = Step.of("next", String.class, String.class, (s, ctx) -> s);

        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
            .then(slow)
            .then(next)
            .withDeadline(30)
            .build();

        Result<String> result = pipeline.execute("x");

        assertThat(result).isInstanceOf(Failure.class);
        Throwable cause = ((Failure<String>) result).cause();
        assertThat(cause).isInstanceOf(PipelineDeadlineExceededException.class);
        assertThat(((PipelineDeadlineExceededException) cause).deadlineMs()).isEqualTo(30L);
    }

    @Test
    void deadline_is_checked_between_foreach_items() {
        AtomicBoolean thirdItemRan = new AtomicBoolean(false);

        Step<String, String> slowItem = Step.of("item", String.class, String.class, (s, ctx) -> {
            if ("item-2".equals(s)) thirdItemRan.set(true);
            if ("item-0".equals(s)) sleepUninterruptibly(150);
            return s;
        });

        @SuppressWarnings({"unchecked", "rawtypes"})
        Step<String, List<String>> toList = (Step) Step.of("to-list", String.class, List.class,
            (s, ctx) -> List.of("item-0", "item-1", "item-2"));

        @SuppressWarnings({"unchecked", "rawtypes"})
        Pipeline<String, ?> pipeline = (Pipeline<String, ?>) (Pipeline) PipelineBuilder.start(String.class)
            .then(toList)
            .foreach(slowItem)
            .withDeadline(30)
            .build();

        Result<?> result = pipeline.execute("x");

        assertThat(result).isInstanceOf(Failure.class);
        assertThat(((Failure<?>) result).cause()).isInstanceOf(PipelineDeadlineExceededException.class);
        assertThat(thirdItemRan.get()).isFalse();
    }

    @Test
    void deadline_propagates_into_branch_arms() {
        AtomicBoolean armStepRan = new AtomicBoolean(false);

        Step<String, String> slow = Step.of("slow", String.class, String.class, (s, ctx) -> {
            sleepUninterruptibly(150);
            return s;
        });
        Step<String, String> armStep = Step.of("arm-step", String.class, String.class, (s, ctx) -> {
            armStepRan.set(true);
            return s;
        });

        Pipeline<String, String> armPipeline = PipelineBuilder.start(String.class)
            .then(armStep)
            .build();

        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
            .then(slow)
            .branch("route", (s, ctx) -> true, armPipeline, armPipeline)
            .withDeadline(20)
            .build();

        Result<String> result = pipeline.execute("x");

        assertThat(result).isInstanceOf(Failure.class);
        assertThat(((Failure<String>) result).cause()).isInstanceOf(PipelineDeadlineExceededException.class);
        assertThat(armStepRan.get()).isFalse();
    }

    @Test
    void deadline_enforced_while_waiting_for_parallel_branch() {
        // One branch sleeps past the deadline; the pipeline must surface a deadline failure
        // rather than blocking until the slow branch finishes.
        Step<String, String> slow = Step.of("slow-par", String.class, String.class, (s, ctx) -> {
            sleepUninterruptibly(500);
            return s;
        });
        Step<String, String> fast = Step.of("fast-par", String.class, String.class, (s, ctx) -> s);

        java.util.concurrent.ExecutorService exec = java.util.concurrent.Executors.newFixedThreadPool(2);
        try {
            Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
                .parallel2(String.class, (a, b) -> a, slow, fast)
                .withExecutor(exec)
                .withDeadline(50)
                .build();

            long start = System.currentTimeMillis();
            Result<String> result = pipeline.execute("x");
            long elapsed = System.currentTimeMillis() - start;

            assertThat(result).isInstanceOf(Failure.class);
            assertThat(((Failure<String>) result).cause()).isInstanceOf(PipelineDeadlineExceededException.class);
            assertThat(((Failure<String>) result).failedStepId()).isEqualTo("pipeline.deadline");
            // Should not have waited the full 500ms for the slow branch
            assertThat(elapsed).isLessThan(400);
        } finally {
            exec.shutdown();
        }
    }

    @Test
    void no_deadline_by_default() {
        Step<String, String> step = Step.of("step", String.class, String.class, (s, ctx) -> {
            sleepUninterruptibly(30);
            return s;
        });

        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
            .then(step)
            .build();

        Result<String> result = pipeline.execute("hello");

        assertThat(result).isInstanceOf(Success.class);
    }

    // -------------------------------------------------------------------------
    // withDeadline validation
    // -------------------------------------------------------------------------

    @Test
    void withDeadline_zero_throws_illegal_argument() {
        assertThatThrownBy(() ->
            PipelineBuilder.start(String.class)
                .then(Step.of("s", String.class, String.class, (s, ctx) -> s))
                .withDeadline(0)
        ).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void withDeadline_negative_throws_illegal_argument() {
        assertThatThrownBy(() ->
            PipelineBuilder.start(String.class)
                .then(Step.of("s", String.class, String.class, (s, ctx) -> s))
                .withDeadline(-1)
        ).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void withDeadline_timeunit_overload_works() {
        Step<String, String> fast = Step.of("fast", String.class, String.class, (s, ctx) -> s);

        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
            .then(fast)
            .withDeadline(5, TimeUnit.SECONDS)
            .build();

        assertThat(pipeline.execute("ok")).isInstanceOf(Success.class);
    }

    @Test
    void withDeadline_set_before_structural_method_is_preserved() {
        AtomicBoolean secondRan = new AtomicBoolean(false);

        Step<String, String> slow = Step.of("slow", String.class, String.class, (s, ctx) -> {
            sleepUninterruptibly(150);
            return s;
        });
        Step<String, String> second = Step.of("second", String.class, String.class, (s, ctx) -> {
            secondRan.set(true);
            return s;
        });

        // withDeadline set on the builder BEFORE the second .then() — must still apply
        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
            .then(slow)
            .withDeadline(20)
            .then(second)
            .build();

        Result<String> result = pipeline.execute("x");

        assertThat(result).isInstanceOf(Failure.class);
        assertThat(((Failure<String>) result).cause()).isInstanceOf(PipelineDeadlineExceededException.class);
        assertThat(secondRan.get()).isFalse();
    }

    private static void sleepUninterruptibly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
