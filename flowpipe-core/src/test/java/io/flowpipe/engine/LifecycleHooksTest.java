package io.flowpipe.engine;

import io.flowpipe.api.Failure;
import io.flowpipe.api.PipelineLifecycle;
import io.flowpipe.api.Result;
import io.flowpipe.api.Step;
import io.flowpipe.api.StepContext;
import io.flowpipe.api.Success;
import io.flowpipe.state.StateKey;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class LifecycleHooksTest {

    private static Step<String, String> pass(String id) {
        return Step.of(id, String.class, String.class, (input, ctx) -> input);
    }

    private static Step<String, String> throwing(String id, RuntimeException ex) {
        return Step.of(id, String.class, String.class, (input, ctx) -> { throw ex; });
    }

    // -------------------------------------------------------------------------
    // onStart fires before the first step
    // -------------------------------------------------------------------------

    @Test
    void onStart_fires_before_first_step() {
        StateKey<Boolean> flag = StateKey.of("started", Boolean.class);
        List<Boolean> flagAtExecution = new ArrayList<>();

        PipelineLifecycle<String, String> lifecycle = new PipelineLifecycle<>() {
            @Override
            public void onStart(String input, StepContext ctx) {
                ctx.state().set(flag, true);
            }
        };

        Step<String, String> step = Step.of("check", String.class, String.class, (input, ctx) -> {
            flagAtExecution.add(Boolean.TRUE.equals(ctx.state().get(flag)));
            return input;
        });

        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
            .then(step)
            .withLifecycle(lifecycle)
            .build();

        pipeline.execute("hello");

        assertThat(flagAtExecution).containsExactly(true);
    }

    // -------------------------------------------------------------------------
    // onFinish fires with Success result
    // -------------------------------------------------------------------------

    @Test
    void onFinish_receives_success_result() {
        AtomicReference<Result<String>> captured = new AtomicReference<>();

        PipelineLifecycle<String, String> lifecycle = new PipelineLifecycle<>() {
            @Override
            public void onFinish(Result<String> result, StepContext ctx) {
                captured.set(result);
            }
        };

        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
            .then(pass("a"))
            .withLifecycle(lifecycle)
            .build();

        pipeline.execute("hi");

        assertThat(captured.get()).isInstanceOf(Success.class);
        assertThat(((Success<String>) captured.get()).value()).isEqualTo("hi");
    }

    // -------------------------------------------------------------------------
    // onFinish fires with Failure result when a step throws
    // -------------------------------------------------------------------------

    @Test
    void onFinish_receives_failure_result() {
        AtomicReference<Result<String>> captured = new AtomicReference<>();
        RuntimeException boom = new RuntimeException("boom");

        PipelineLifecycle<String, String> lifecycle = new PipelineLifecycle<>() {
            @Override
            public void onFinish(Result<String> result, StepContext ctx) {
                captured.set(result);
            }
        };

        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
            .then(throwing("explode", boom))
            .withLifecycle(lifecycle)
            .build();

        pipeline.execute("x");

        assertThat(captured.get()).isInstanceOf(Failure.class);
        assertThat(((Failure<String>) captured.get()).cause()).isSameAs(boom);
    }

    // -------------------------------------------------------------------------
    // onError fires after onFinish, only on failure
    // -------------------------------------------------------------------------

    @Test
    void onError_fires_after_onFinish_only_on_failure() {
        List<String> order = new ArrayList<>();
        RuntimeException boom = new RuntimeException("boom");

        PipelineLifecycle<String, String> lifecycle = new PipelineLifecycle<>() {
            @Override
            public void onFinish(Result<String> result, StepContext ctx) {
                order.add("onFinish");
            }
            @Override
            public void onError(Failure<String> failure, StepContext ctx) {
                order.add("onError");
                assertThat(failure.cause()).isSameAs(boom);
            }
        };

        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
            .then(throwing("fail", boom))
            .withLifecycle(lifecycle)
            .build();

        pipeline.execute("x");

        assertThat(order).containsExactly("onFinish", "onError");
    }

    @Test
    void onError_not_called_on_success() {
        List<String> errorCalls = new ArrayList<>();

        PipelineLifecycle<String, String> lifecycle = new PipelineLifecycle<>() {
            @Override
            public void onError(Failure<String> failure, StepContext ctx) {
                errorCalls.add("onError");
            }
        };

        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
            .then(pass("a"))
            .withLifecycle(lifecycle)
            .build();

        pipeline.execute("hello");

        assertThat(errorCalls).isEmpty();
    }

    // -------------------------------------------------------------------------
    // onStart exception halts steps but still fires onFinish and onError
    // -------------------------------------------------------------------------

    @Test
    void onStart_exception_returns_failure_with_pipeline_onStart_id() {
        RuntimeException boom = new RuntimeException("auth failed");
        List<String> stepCalls = new ArrayList<>();
        List<String> hookCalls = new ArrayList<>();

        PipelineLifecycle<String, String> lifecycle = new PipelineLifecycle<>() {
            @Override
            public void onStart(String input, StepContext ctx) {
                throw boom;
            }
            @Override
            public void onFinish(Result<String> result, StepContext ctx) {
                hookCalls.add("onFinish");
            }
            @Override
            public void onError(Failure<String> failure, StepContext ctx) {
                hookCalls.add("onError");
            }
        };

        Step<String, String> step = Step.of("never", String.class, String.class, (input, ctx) -> {
            stepCalls.add("executed");
            return input;
        });

        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
            .then(step)
            .withLifecycle(lifecycle)
            .build();

        Result<String> result = pipeline.execute("hello");

        assertThat(result).isInstanceOf(Failure.class);
        Failure<String> failure = (Failure<String>) result;
        assertThat(failure.cause()).isSameAs(boom);
        assertThat(failure.failedStepId()).isEqualTo("pipeline.onStart");
        assertThat(stepCalls).isEmpty();
        assertThat(hookCalls).containsExactly("onFinish", "onError");
    }

    // -------------------------------------------------------------------------
    // onFinish exception does not change result; does not suppress onError
    // -------------------------------------------------------------------------

    @Test
    void onFinish_exception_does_not_change_success_result() {
        PipelineLifecycle<String, String> lifecycle = new PipelineLifecycle<>() {
            @Override
            public void onFinish(Result<String> result, StepContext ctx) {
                throw new RuntimeException("finish failed");
            }
        };

        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
            .then(pass("a"))
            .withLifecycle(lifecycle)
            .build();

        Result<String> result = pipeline.execute("hello");

        assertThat(result).isInstanceOf(Success.class);
        assertThat(((Success<String>) result).value()).isEqualTo("hello");
    }

    @Test
    void onFinish_exception_does_not_suppress_onError() {
        List<String> hookCalls = new ArrayList<>();
        RuntimeException stepBoom = new RuntimeException("step fail");

        PipelineLifecycle<String, String> lifecycle = new PipelineLifecycle<>() {
            @Override
            public void onFinish(Result<String> result, StepContext ctx) {
                hookCalls.add("onFinish");
                throw new RuntimeException("finish failed");
            }
            @Override
            public void onError(Failure<String> failure, StepContext ctx) {
                hookCalls.add("onError");
            }
        };

        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
            .then(throwing("fail", stepBoom))
            .withLifecycle(lifecycle)
            .build();

        pipeline.execute("x");

        assertThat(hookCalls).containsExactly("onFinish", "onError");
    }

    // -------------------------------------------------------------------------
    // onError exception does not change failure result
    // -------------------------------------------------------------------------

    @Test
    void onError_exception_does_not_change_failure_result() {
        RuntimeException stepBoom = new RuntimeException("step fail");

        PipelineLifecycle<String, String> lifecycle = new PipelineLifecycle<>() {
            @Override
            public void onError(Failure<String> failure, StepContext ctx) {
                throw new RuntimeException("error hook failed");
            }
        };

        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
            .then(throwing("fail", stepBoom))
            .withLifecycle(lifecycle)
            .build();

        Result<String> result = pipeline.execute("x");

        assertThat(result).isInstanceOf(Failure.class);
        assertThat(((Failure<String>) result).cause()).isSameAs(stepBoom);
    }
}
