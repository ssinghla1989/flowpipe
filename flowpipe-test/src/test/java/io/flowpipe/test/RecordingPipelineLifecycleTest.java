package io.flowpipe.test;

import io.flowpipe.api.Step;
import io.flowpipe.api.Success;
import io.flowpipe.engine.Pipeline;
import io.flowpipe.engine.PipelineBuilder;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RecordingPipelineLifecycleTest {

    private static Step<String, String> pass(String id) {
        return Step.of(id, String.class, String.class, (input, ctx) -> input);
    }

    private static Step<String, String> throwing(String id, RuntimeException ex) {
        return Step.of(id, String.class, String.class, (input, ctx) -> { throw ex; });
    }

    @Test
    void captures_onStart_input() {
        RecordingPipelineLifecycle<String, String> recorder = new RecordingPipelineLifecycle<>();

        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
            .then(pass("a"))
            .withLifecycle(recorder)
            .build();

        pipeline.execute("world");

        assertThat(recorder.onStartInvocations()).hasSize(1);
        assertThat(recorder.onStartInvocations().get(0).input()).isEqualTo("world");
    }

    @Test
    void captures_onFinish_success_result() {
        RecordingPipelineLifecycle<String, String> recorder = new RecordingPipelineLifecycle<>();

        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
            .then(pass("a"))
            .withLifecycle(recorder)
            .build();

        pipeline.execute("hello");

        assertThat(recorder.onFinishInvocations()).hasSize(1);
        assertThat(recorder.onFinishInvocations().get(0).result()).isInstanceOf(Success.class);
    }

    @Test
    void captures_onError_failure_cause() {
        RuntimeException boom = new RuntimeException("recording test");
        RecordingPipelineLifecycle<String, String> recorder = new RecordingPipelineLifecycle<>();

        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
            .then(throwing("fail", boom))
            .withLifecycle(recorder)
            .build();

        pipeline.execute("x");

        assertThat(recorder.onErrorInvocations()).hasSize(1);
        assertThat(recorder.onErrorInvocations().get(0).failure().cause()).isSameAs(boom);
    }

    @Test
    void onError_not_captured_on_success() {
        RecordingPipelineLifecycle<String, String> recorder = new RecordingPipelineLifecycle<>();

        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
            .then(pass("a"))
            .withLifecycle(recorder)
            .build();

        pipeline.execute("hello");

        assertThat(recorder.onErrorInvocations()).isEmpty();
    }
}
