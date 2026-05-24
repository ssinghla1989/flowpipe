package io.flowpipe.engine;

import io.flowpipe.api.PipelineLifecycle;
import io.flowpipe.api.Step;
import io.flowpipe.api.StepContext;
import io.flowpipe.api.Result;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class LifecycleNotCalledForSubPipelinesTest {

    private static Step<Integer, String> intToStr(String id) {
        return Step.of(id, Integer.class, String.class, (i, ctx) -> "v" + i);
    }

    @Test
    void onStart_called_exactly_once_for_outer_pipeline_not_for_branch_arms() {
        AtomicInteger onStartCount = new AtomicInteger(0);

        PipelineLifecycle<Integer, String> lifecycle = new PipelineLifecycle<>() {
            @Override
            public void onStart(Integer input, StepContext ctx) {
                onStartCount.incrementAndGet();
            }
        };

        Pipeline<Integer, String> trueArm = PipelineBuilder.start(Integer.class)
            .then(intToStr("pos"))
            .build();

        Pipeline<Integer, String> falseArm = PipelineBuilder.start(Integer.class)
            .then(intToStr("neg"))
            .build();

        Pipeline<Integer, String> pipeline = PipelineBuilder.start(Integer.class)
            .branch("check", (val, ctx) -> val > 0, trueArm, falseArm)
            .withLifecycle(lifecycle)
            .build();

        pipeline.execute(5);
        pipeline.execute(-3);

        assertThat(onStartCount.get()).isEqualTo(2);
    }

    @Test
    void onFinish_called_exactly_once_per_top_level_execute_not_per_arm() {
        AtomicInteger onFinishCount = new AtomicInteger(0);

        PipelineLifecycle<Integer, String> lifecycle = new PipelineLifecycle<>() {
            @Override
            public void onFinish(Result<String> result, StepContext ctx) {
                onFinishCount.incrementAndGet();
            }
        };

        Pipeline<Integer, String> trueArm = PipelineBuilder.start(Integer.class)
            .then(intToStr("pos"))
            .build();

        Pipeline<Integer, String> falseArm = PipelineBuilder.start(Integer.class)
            .then(intToStr("neg"))
            .build();

        Pipeline<Integer, String> pipeline = PipelineBuilder.start(Integer.class)
            .branch("check", (val, ctx) -> val > 0, trueArm, falseArm)
            .withLifecycle(lifecycle)
            .build();

        pipeline.execute(5);

        assertThat(onFinishCount.get()).isEqualTo(1);
    }
}
