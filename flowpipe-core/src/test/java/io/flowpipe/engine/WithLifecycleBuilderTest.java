package io.flowpipe.engine;

import io.flowpipe.api.PipelineLifecycle;
import io.flowpipe.api.Step;
import io.flowpipe.api.Success;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WithLifecycleBuilderTest {

    private static Step<String, String> pass(String id) {
        return Step.builder(id, String.class, String.class).execute((input, ctx) -> input).build();
    }

    @Test
    void withLifecycle_null_throws_NullPointerException() {
        assertThatThrownBy(() ->
            PipelineBuilder.start(String.class)
                .then(pass("a"))
                .withLifecycle(null))
            .isInstanceOf(NullPointerException.class);
    }

    @Test
    void withLifecycle_on_consumed_builder_throws_IllegalStateException() {
        PipelineLifecycle<String, String> lc = new PipelineLifecycle<>() {};
        PipelineBuilder<String, String> first = PipelineBuilder.start(String.class);
        first.then(pass("a"));

        assertThatThrownBy(() -> first.withLifecycle(lc))
            .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void pipeline_without_lifecycle_executes_without_error() {
        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
            .then(pass("a"))
            .build();

        assertThat(pipeline.execute("hello")).isInstanceOf(Success.class);
    }

    @Test
    void subsequent_withLifecycle_calls_replace_previous() {
        AtomicInteger firstCount = new AtomicInteger(0);
        AtomicInteger secondCount = new AtomicInteger(0);

        PipelineLifecycle<String, String> first = new PipelineLifecycle<>() {
            @Override
            public void onStart(String input, io.flowpipe.api.StepContext ctx) {
                firstCount.incrementAndGet();
            }
        };
        PipelineLifecycle<String, String> second = new PipelineLifecycle<>() {
            @Override
            public void onStart(String input, io.flowpipe.api.StepContext ctx) {
                secondCount.incrementAndGet();
            }
        };

        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
            .then(pass("a"))
            .withLifecycle(first)
            .withLifecycle(second)
            .build();

        pipeline.execute("hello");

        assertThat(firstCount.get()).isEqualTo(0);
        assertThat(secondCount.get()).isEqualTo(1);
    }
}
