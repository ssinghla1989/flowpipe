package io.flowpipe.engine;

import io.flowpipe.api.Step;
import io.flowpipe.api.Success;
import io.flowpipe.api.Result;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PipelineBuilderAliasTest {

    // -------------------------------------------------------------------------
    // 1. Pipeline.builder() produces a working pipeline
    // -------------------------------------------------------------------------

    @Test
    void pipeline_builder_alias_produces_equivalent_pipeline() {
        Step<String, String> step = Step.of("up", String.class, String.class,
            (in, ctx) -> in.toUpperCase());

        Pipeline<String, String> viaAlias = Pipeline.builder(String.class)
            .then(step)
            .build();
        Pipeline<String, String> viaStart = PipelineBuilder.start(String.class)
            .then(step)
            .build();

        Result<String> r1 = viaAlias.execute("hello");
        Result<String> r2 = viaStart.execute("hello");

        assertThat(r1).isInstanceOf(Success.class);
        assertThat(r2).isInstanceOf(Success.class);
        assertThat(((Success<String>) r1).value()).isEqualTo(((Success<String>) r2).value());
    }

    // -------------------------------------------------------------------------
    // 2. Pipeline.builder(null) throws NullPointerException
    // -------------------------------------------------------------------------

    @Test
    void pipeline_builder_null_input_type_throws() {
        assertThatThrownBy(() -> Pipeline.builder(null))
            .isInstanceOf(NullPointerException.class);
    }

    // -------------------------------------------------------------------------
    // 3. PipelineBuilder.start() still works — no regression
    // -------------------------------------------------------------------------

    @Test
    void pipeline_builder_start_still_works() {
        Step<Integer, Integer> step = Step.of("id", Integer.class, Integer.class, (i, ctx) -> i);
        Pipeline<Integer, Integer> p = PipelineBuilder.start(Integer.class).then(step).build();
        Result<Integer> result = p.execute(42);
        assertThat(result).isInstanceOf(Success.class);
        assertThat(((Success<Integer>) result).value()).isEqualTo(42);
    }
}
