package io.flowpipe.engine;

import io.flowpipe.api.Step;
import io.flowpipe.api.StepDescriptor;
import io.flowpipe.api.StepContext;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PipelineCompositionTest {

    @Test
    void compatible_chain_compiles_and_builds() {
        Step<String, Integer> parse = Step.of("parse", String.class, Integer.class,
            (s, ctx) -> Integer.parseInt(s));
        Step<Integer, String> render = Step.of("render", Integer.class, String.class,
            (i, ctx) -> Integer.toString(i));

        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
            .then(parse)
            .then(render)
            .build();

        assertThat(pipeline.inputType()).isEqualTo(String.class);
        assertThat(pipeline.outputType()).isEqualTo(String.class);
    }

    @Test
    void build_rejects_empty_pipeline() {
        assertThatThrownBy(() -> PipelineBuilder.start(String.class).build())
            .isInstanceOf(PipelineBuildException.class)
            .hasMessageContaining("empty pipeline");
    }

    @Test
    void build_rejects_duplicate_step_ids() {
        Step<String, String> a = Step.of("normalize", String.class, String.class,
            (s, ctx) -> s);
        Step<String, String> b = Step.of("normalize", String.class, String.class,
            (s, ctx) -> s);

        assertThatThrownBy(() -> PipelineBuilder.start(String.class)
            .then(a)
            .then(b)
            .build())
            .isInstanceOf(PipelineBuildException.class)
            .hasMessageContaining("normalize");
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void then_rejects_step_to_step_type_mismatch_from_raw_coercion() {
        // Construct a step whose generic chain says <String, String> but whose descriptor
        // declares outputType Integer.class — only possible via raw-type escape. The
        // following step's declared inputType (String) catches the lie at .then() time.
        StepDescriptor<String, Integer> mismatchedDescriptor =
            StepDescriptor.builder("lying", String.class, Integer.class).build();
        Step rawStep = new Step() {
            @Override public StepDescriptor describe() { return mismatchedDescriptor; }
            @Override public Object execute(Object input, StepContext ctx) { return input; }
        };
        Step<String, String> coerced = (Step<String, String>) rawStep;
        Step<String, String> next = Step.of("next", String.class, String.class,
            (s, ctx) -> s);

        assertThatThrownBy(() -> PipelineBuilder.start(String.class)
            .then(coerced)
            .then(next))
            .isInstanceOf(PipelineBuildException.class)
            .hasMessageContaining("next")
            .hasMessageContaining("String")
            .hasMessageContaining("Integer");
    }

    @Test
    void builder_rejects_then_after_build() {
        Step<String, String> a = Step.of("a", String.class, String.class, (s, ctx) -> s);
        PipelineBuilder<String, String> builder = PipelineBuilder.start(String.class).then(a);
        builder.build();

        Step<String, String> b = Step.of("b", String.class, String.class, (s, ctx) -> s);
        assertThatThrownBy(() -> builder.then(b))
            .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void builder_rejects_double_build() {
        Step<String, String> a = Step.of("a", String.class, String.class, (s, ctx) -> s);
        PipelineBuilder<String, String> builder = PipelineBuilder.start(String.class).then(a);
        builder.build();

        assertThatThrownBy(builder::build)
            .isInstanceOf(IllegalStateException.class);
    }
}
