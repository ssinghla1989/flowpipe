package io.flowpipe.observability;

import ch.qos.logback.classic.spi.ILoggingEvent;
import io.flowpipe.api.Failure;
import io.flowpipe.api.Result;
import io.flowpipe.api.Step;
import io.flowpipe.api.Success;
import io.flowpipe.engine.Pipeline;
import io.flowpipe.engine.PipelineBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class RecorderExceptionIsolationTest {

    private Slf4jTestAppender appender;

    @BeforeEach
    void attach() {
        appender = Slf4jTestAppender.attachToEngine();
    }

    @AfterEach
    void detach() {
        appender.detach();
    }

    @Test
    void throwing_recorder_does_not_turn_success_into_failure() {
        MetricsRecorder thrower = new MetricsRecorder() {
            @Override public void recordStepDuration(String stepId, long durationNanos) {
                throw new RuntimeException("boom");
            }
            @Override public void recordStepAttempts(String stepId, int attempts) {
                throw new RuntimeException("boom");
            }
            @Override public void recordStepOutcome(String stepId, StepOutcome outcome) {
                throw new RuntimeException("boom");
            }
            @Override public void recordRetryAttempt(String stepId, int attemptNumber) {}
        };

        Step<Integer, Integer> a = Step.of("a", Integer.class, Integer.class, (i, ctx) -> i + 1);
        Step<Integer, Integer> b = Step.of("b", Integer.class, Integer.class, (i, ctx) -> i * 2);

        Pipeline<Integer, Integer> pipeline = PipelineBuilder.start(Integer.class)
            .then(a).then(b)
            .withMetrics(thrower)
            .build();
        Result<Integer> result = pipeline.execute(10);

        assertThat(result).isInstanceOf(Success.class);
        assertThat(((Success<Integer>) result).value()).isEqualTo(22);

        List<ILoggingEvent> recorderFailures = appender.events("metrics.recorder_failed");
        assertThat(recorderFailures).isNotEmpty();
    }

    @Test
    void throwing_recorder_does_not_mask_a_genuine_step_failure() {
        MetricsRecorder thrower = new MetricsRecorder() {
            @Override public void recordStepDuration(String stepId, long durationNanos) {
                throw new RuntimeException("recorder boom");
            }
            @Override public void recordStepAttempts(String stepId, int attempts) {
                throw new RuntimeException("recorder boom");
            }
            @Override public void recordStepOutcome(String stepId, StepOutcome outcome) {
                throw new RuntimeException("recorder boom");
            }
            @Override public void recordRetryAttempt(String stepId, int attemptNumber) {}
        };

        IllegalStateException real = new IllegalStateException("real failure");
        Step<Integer, Integer> bad = Step.of("bad", Integer.class, Integer.class,
            (i, ctx) -> { throw real; });

        Pipeline<Integer, Integer> pipeline = PipelineBuilder.start(Integer.class)
            .then(bad)
            .withMetrics(thrower)
            .build();
        Result<Integer> result = pipeline.execute(1);

        assertThat(result).isInstanceOf(Failure.class);
        Failure<Integer> failure = (Failure<Integer>) result;
        assertThat(failure.cause()).isSameAs(real);
        assertThat(failure.failedStepId()).isEqualTo("bad");

        assertThat(appender.events("metrics.recorder_failed")).isNotEmpty();
        assertThat(appender.events("step.error")).hasSize(1);
    }
}
