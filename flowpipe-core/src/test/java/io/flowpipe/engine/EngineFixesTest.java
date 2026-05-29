package io.flowpipe.engine;

import io.flowpipe.api.Failure;
import io.flowpipe.api.PipelineLifecycle;
import io.flowpipe.api.Result;
import io.flowpipe.api.Step;
import io.flowpipe.api.StepContext;
import io.flowpipe.api.StepDescriptor;
import io.flowpipe.api.Success;
import io.flowpipe.observability.SpanRecorder;
import io.flowpipe.observability.StepOutcome;
import io.flowpipe.state.RequestContext;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests covering the 4 engine correctness fixes and SpanRecorder SPI.
 */
class EngineFixesTest {

    // -------------------------------------------------------------------------
    // Fix 1: invokeStep null-check on step output
    // -------------------------------------------------------------------------

    @Test
    void step_returning_null_surfaces_as_failure_with_clear_message() {
        Step<String, String> nullStep = Step.of("null-returner", String.class, String.class,
            (input, ctx) -> null);

        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
            .then(nullStep)
            .build();

        Result<String> result = pipeline.execute("input");

        assertThat(result).isInstanceOf(Failure.class);
        Failure<String> failure = (Failure<String>) result;
        assertThat(failure.cause()).isInstanceOf(NullPointerException.class);
        assertThat(failure.cause().getMessage()).contains("null-returner");
        assertThat(failure.failedStepId()).isEqualTo("null-returner");
    }

    // -------------------------------------------------------------------------
    // Fix 2: InterruptedException restores interrupt flag
    // -------------------------------------------------------------------------

    @Test
    void step_throwing_interrupted_exception_restores_interrupt_flag() throws InterruptedException {
        Step<String, String> interruptingStep = new Step<>() {
            @Override
            public StepDescriptor<String, String> describe() {
                return StepDescriptor.builder("interrupt", String.class, String.class).build();
            }
            @Override
            public String execute(String input, StepContext ctx) throws Exception {
                throw new InterruptedException("simulated");
            }
        };

        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
            .then(interruptingStep)
            .build();

        AtomicBoolean interruptFlagAfter = new AtomicBoolean(false);
        Thread thread = new Thread(() -> {
            pipeline.execute("input");
            interruptFlagAfter.set(Thread.currentThread().isInterrupted());
        });
        thread.start();
        thread.join(5000);

        assertThat(interruptFlagAfter.get()).isTrue();
    }

    // -------------------------------------------------------------------------
    // Fix 3: onStart failure still calls onFinish and onError
    // -------------------------------------------------------------------------

    @Test
    void lifecycle_onFinish_and_onError_called_even_when_onStart_throws() {
        AtomicBoolean onFinishCalled = new AtomicBoolean(false);
        AtomicBoolean onErrorCalled = new AtomicBoolean(false);

        PipelineLifecycle<String, String> lifecycle = new PipelineLifecycle<>() {
            @Override
            public void onStart(String input, StepContext ctx) {
                throw new RuntimeException("onStart boom");
            }
            @Override
            public void onFinish(Result<String> result, StepContext ctx) {
                onFinishCalled.set(true);
            }
            @Override
            public void onError(Failure<String> failure, StepContext ctx) {
                onErrorCalled.set(true);
            }
        };

        Step<String, String> step = Step.of("step", String.class, String.class, (s, ctx) -> s);
        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
            .then(step)
            .withLifecycle(lifecycle)
            .build();

        Result<String> result = pipeline.execute("input");

        assertThat(result).isInstanceOf(Failure.class);
        assertThat(((Failure<String>) result).failedStepId()).isEqualTo("pipeline.onStart");
        assertThat(onFinishCalled.get()).isTrue();
        assertThat(onErrorCalled.get()).isTrue();
    }

    // -------------------------------------------------------------------------
    // Fix 4: branch predicate failure emits observability
    // -------------------------------------------------------------------------

    @Test
    void branch_predicate_exception_surfaces_as_failure_with_branch_id() {
        Step<String, String> step = Step.of("step", String.class, String.class, (s, ctx) -> s);
        Pipeline<String, String> ifTrue = PipelineBuilder.start(String.class).then(step).build();
        Pipeline<String, String> ifFalse = PipelineBuilder.start(String.class).then(step).build();

        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
            .branch("router", (input, ctx) -> { throw new RuntimeException("predicate boom"); },
                ifTrue, ifFalse)
            .build();

        Result<String> result = pipeline.execute("input");

        assertThat(result).isInstanceOf(Failure.class);
        Failure<String> failure = (Failure<String>) result;
        assertThat(failure.failedStepId()).isEqualTo("router");
        assertThat(failure.cause()).hasMessage("predicate boom");
    }

    // -------------------------------------------------------------------------
    // SpanRecorder SPI
    // -------------------------------------------------------------------------

    @Test
    void span_recorder_receives_start_and_finish_for_each_step() {
        List<String> events = new ArrayList<>();

        SpanRecorder recorder = new SpanRecorder() {
            @Override
            public Object startStep(String stepId, RequestContext context) {
                events.add("start:" + stepId);
                return stepId + "-span";
            }
            @Override
            public void finishStep(Object span, StepOutcome outcome, Throwable cause) {
                events.add("finish:" + span + ":" + outcome);
            }
        };

        Step<String, String> a = Step.of("a", String.class, String.class, (s, ctx) -> s + "A");
        Step<String, String> b = Step.of("b", String.class, String.class, (s, ctx) -> s + "B");

        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
            .then(a)
            .then(b)
            .withTracing(recorder)
            .build();

        pipeline.execute("x");

        assertThat(events).containsExactly(
            "start:a", "finish:a-span:SUCCESS",
            "start:b", "finish:b-span:SUCCESS"
        );
    }

    @Test
    void span_recorder_receives_failure_outcome_on_step_error() {
        AtomicReference<StepOutcome> capturedOutcome = new AtomicReference<>();
        AtomicReference<Throwable> capturedCause = new AtomicReference<>();

        SpanRecorder recorder = new SpanRecorder() {
            @Override
            public Object startStep(String stepId, RequestContext context) {
                return null;
            }
            @Override
            public void finishStep(Object span, StepOutcome outcome, Throwable cause) {
                capturedOutcome.set(outcome);
                capturedCause.set(cause);
            }
        };

        RuntimeException boom = new RuntimeException("boom");
        Step<String, String> failingStep = Step.of("fail", String.class, String.class,
            (input, ctx) -> { throw boom; });

        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
            .then(failingStep)
            .withTracing(recorder)
            .build();

        pipeline.execute("input");

        assertThat(capturedOutcome.get()).isEqualTo(StepOutcome.FAILURE);
        assertThat(capturedCause.get()).isSameAs(boom);
    }

    @Test
    void span_recorder_receives_skipped_outcome_for_branch_not_taken() {
        List<String> finishEvents = new ArrayList<>();

        SpanRecorder recorder = new SpanRecorder() {
            @Override
            public Object startStep(String stepId, RequestContext context) {
                return stepId;
            }
            @Override
            public void finishStep(Object span, StepOutcome outcome, Throwable cause) {
                finishEvents.add(span + ":" + outcome);
            }
        };

        Step<Integer, Integer> incrementStep = Step.of("increment", Integer.class, Integer.class,
            (n, ctx) -> n + 1);
        Step<Integer, Integer> decrementStep = Step.of("decrement", Integer.class, Integer.class,
            (n, ctx) -> n - 1);

        Pipeline<Integer, Integer> ifTrue = PipelineBuilder.start(Integer.class)
            .then(incrementStep).build();
        Pipeline<Integer, Integer> ifFalse = PipelineBuilder.start(Integer.class)
            .then(decrementStep).build();

        Pipeline<Integer, Integer> pipeline = PipelineBuilder.start(Integer.class)
            .branch("check", (n, ctx) -> n > 0, ifTrue, ifFalse)
            .withTracing(recorder)
            .build();

        pipeline.execute(5);

        // The outer recorder sees SKIPPED spans via emitSkipped (which passes the outer spanRecorder).
        // Execution spans for the taken arm use the arm pipeline's own (NoOp) recorder.
        assertThat(finishEvents).contains("decrement:SKIPPED");
    }

    @Test
    void span_recorder_exceptions_are_suppressed_and_do_not_affect_pipeline() {
        SpanRecorder badRecorder = new SpanRecorder() {
            @Override
            public Object startStep(String stepId, RequestContext context) {
                throw new RuntimeException("recorder exploded");
            }
            @Override
            public void finishStep(Object span, StepOutcome outcome, Throwable cause) {
                throw new RuntimeException("finish exploded");
            }
        };

        Step<String, String> step = Step.of("step", String.class, String.class, (s, ctx) -> s);
        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
            .then(step)
            .withTracing(badRecorder)
            .build();

        Result<String> result = pipeline.execute("hello");

        assertThat(result).isInstanceOf(Success.class);
        assertThat(((Success<String>) result).value()).isEqualTo("hello");
    }
}
