package io.flowpipe.observability;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import io.flowpipe.api.Step;
import io.flowpipe.engine.Pipeline;
import io.flowpipe.engine.PipelineBuilder;
import io.flowpipe.state.ContextKey;
import io.flowpipe.state.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class StepLoggingTest {

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
    void step_start_log_carries_step_id_attempt_and_context_fields() {
        ContextKey<String> TRACE_ID = ContextKey.of("traceId", String.class);
        Step<String, String> enrich = Step.builder("enrich", String.class, String.class).execute((s, ctx) -> s).build();

        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class).then(enrich).build();
        pipeline.execute("x", RequestContext.builder().put(TRACE_ID, "abc-123").build());

        List<ILoggingEvent> starts = appender.events("step.start");
        assertThat(starts).hasSize(1);
        Map<String, Object> fields = Slf4jTestAppender.fields(starts.get(0));
        assertThat(fields)
            .containsEntry("step.id", "enrich")
            .containsEntry("step.attempt", 1)
            .containsEntry("traceId", "abc-123");
    }

    @Test
    void step_finish_log_on_success_carries_duration_and_outcome() {
        Step<String, String> compute = Step.builder("compute", String.class, String.class).execute((s, ctx) -> s).build();

        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class).then(compute).build();
        pipeline.execute("hello");

        List<ILoggingEvent> finishes = appender.events("step.finish");
        assertThat(finishes).hasSize(1);
        Map<String, Object> fields = Slf4jTestAppender.fields(finishes.get(0));
        assertThat(fields)
            .containsEntry("step.id", "compute")
            .containsEntry("step.outcome", "success");
        Long durationMs = (Long) fields.get("step.duration_ms");
        assertThat(durationMs).isNotNull().isGreaterThanOrEqualTo(0L);
    }

    @Test
    void step_error_log_on_failure_carries_error_class_message_and_no_finish_for_failing_step() {
        Step<String, String> unstable = Step.builder("unstable", String.class, String.class).execute((s, ctx) -> { throw new IllegalStateException("nope"); }).build();

        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class).then(unstable).build();
        pipeline.execute("x");

        List<ILoggingEvent> errors = appender.events("step.error");
        assertThat(errors).hasSize(1);
        ILoggingEvent error = errors.get(0);
        assertThat(error.getLevel()).isEqualTo(Level.ERROR);
        Map<String, Object> fields = Slf4jTestAppender.fields(error);
        assertThat(fields)
            .containsEntry("step.id", "unstable")
            .containsEntry("step.outcome", "failure")
            .containsEntry("step.error_message", "nope");
        assertThat((String) fields.get("step.error_class")).endsWith("IllegalStateException");

        assertThat(appender.events("step.finish")).isEmpty();
    }
}
