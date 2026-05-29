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
        Step<String, Integer> parse = Step.builder("parse", String.class, Integer.class).execute((s, ctx) -> Integer.parseInt(s)).build();
        Step<Integer, String> render = Step.builder("render", Integer.class, String.class).execute((i, ctx) -> Integer.toString(i)).build();

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
        Step<String, String> a = Step.builder("normalize", String.class, String.class).execute((s, ctx) -> s).build();
        Step<String, String> b = Step.builder("normalize", String.class, String.class).execute((s, ctx) -> s).build();

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
        Step<String, String> next = Step.builder("next", String.class, String.class).execute((s, ctx) -> s).build();

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
        Step<String, String> a = Step.builder("a", String.class, String.class).execute((s, ctx) -> s).build();
        PipelineBuilder<String, String> builder = PipelineBuilder.start(String.class).then(a);
        builder.build();

        Step<String, String> b = Step.builder("b", String.class, String.class).execute((s, ctx) -> s).build();
        assertThatThrownBy(() -> builder.then(b))
            .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void builder_rejects_double_build() {
        Step<String, String> a = Step.builder("a", String.class, String.class).execute((s, ctx) -> s).build();
        PipelineBuilder<String, String> builder = PipelineBuilder.start(String.class).then(a);
        builder.build();

        assertThatThrownBy(builder::build)
            .isInstanceOf(IllegalStateException.class);
    }
}
