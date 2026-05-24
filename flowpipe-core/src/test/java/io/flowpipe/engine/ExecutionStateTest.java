package io.flowpipe.engine;

import io.flowpipe.api.Step;
import io.flowpipe.state.ContextKey;
import io.flowpipe.state.RequestContext;
import io.flowpipe.state.State;
import io.flowpipe.state.StateKey;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class ExecutionStateTest {

    @Test
    void two_executions_have_isolated_state() {
        StateKey<String> KEY = StateKey.of("k", String.class);

        AtomicReference<String> firstObserved = new AtomicReference<>("initial");
        Step<String, String> writer = Step.of("writer", String.class, String.class,
            (s, ctx) -> {
                firstObserved.set(ctx.state().get(KEY));
                ctx.state().set(KEY, "written-" + s);
                return s;
            });

        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class).then(writer).build();

        pipeline.execute("first");
        assertThat(firstObserved.get()).isNull();

        firstObserved.set("initial");
        pipeline.execute("second");
        assertThat(firstObserved.get()).isNull();
    }

    @Test
    void typed_state_key_get_set_returns_typed_value_with_no_cast() {
        StateKey<Integer> COUNTER = StateKey.of("counter", Integer.class);
        State state = new State();

        state.set(COUNTER, 42);
        Integer value = state.get(COUNTER);

        assertThat(value).isEqualTo(42);
    }

    @Test
    void state_get_on_absent_key_returns_null() {
        StateKey<String> MISSING = StateKey.of("missing", String.class);
        State state = new State();

        assertThat(state.get(MISSING)).isNull();
    }

    @Test
    void request_context_exposes_no_public_mutator() {
        for (Method m : RequestContext.class.getMethods()) {
            if (m.getDeclaringClass() == Object.class) continue;
            String name = m.getName();
            assertThat(name)
                .as("RequestContext should expose no mutator; found " + name)
                .doesNotStartWith("set")
                .doesNotStartWith("put")
                .doesNotStartWith("remove")
                .doesNotStartWith("clear")
                .doesNotStartWith("add");
        }
    }

    @Test
    void two_executions_see_distinct_request_contexts() {
        ContextKey<String> TRACE_ID = ContextKey.of("traceId", String.class);
        AtomicReference<String> observed = new AtomicReference<>();
        Step<String, String> reader = Step.of("reader", String.class, String.class,
            (s, ctx) -> {
                observed.set(ctx.context().get(TRACE_ID));
                return s;
            });

        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class).then(reader).build();

        pipeline.execute("x", RequestContext.builder().put(TRACE_ID, "trace-A").build());
        assertThat(observed.get()).isEqualTo("trace-A");

        pipeline.execute("x", RequestContext.builder().put(TRACE_ID, "trace-B").build());
        assertThat(observed.get()).isEqualTo("trace-B");
    }
}
