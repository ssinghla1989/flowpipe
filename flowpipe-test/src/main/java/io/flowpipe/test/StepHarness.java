package io.flowpipe.test;

import io.flowpipe.api.Step;
import io.flowpipe.api.StepContext;
import io.flowpipe.state.RequestContext;
import io.flowpipe.state.State;
import io.flowpipe.state.StateKey;

import java.util.Objects;

public final class StepHarness {

    private RequestContext context = RequestContext.empty();
    private final State state = new State();

    private StepHarness() {
    }

    public static StepHarness forStep() {
        return new StepHarness();
    }

    public StepHarness withContext(RequestContext context) {
        this.context = Objects.requireNonNull(context, "context");
        return this;
    }

    public <T> StepHarness withState(StateKey<T> key, T value) {
        state.set(key, value);
        return this;
    }

    public <I, O> Outcome<O> invoke(Step<I, O> step, I input) {
        Objects.requireNonNull(step, "step");
        HarnessContext ctx = new HarnessContext(state, context);
        try {
            step.describe().inputValidator().validate(input);
            O output = step.execute(input, ctx);
            step.describe().outputValidator().validate(output);
            return Outcome.success(output, state);
        } catch (Throwable t) {
            return Outcome.thrown(t, state);
        }
    }

    public State state() {
        return state;
    }

    public RequestContext context() {
        return context;
    }

    public static final class Outcome<O> {
        private final O value;
        private final Throwable error;
        private final State state;

        private Outcome(O value, Throwable error, State state) {
            this.value = value;
            this.error = error;
            this.state = state;
        }

        static <O> Outcome<O> success(O value, State state) {
            return new Outcome<>(value, null, state);
        }

        static <O> Outcome<O> thrown(Throwable error, State state) {
            return new Outcome<>(null, error, state);
        }

        public boolean succeeded() {
            return error == null;
        }

        public O value() {
            if (error != null) {
                throw new IllegalStateException("Step threw; no value available", error);
            }
            return value;
        }

        public Throwable error() {
            if (error == null) {
                throw new IllegalStateException("Step did not throw");
            }
            return error;
        }

        public State state() {
            return state;
        }
    }

    private static final class HarnessContext implements StepContext {
        private final State state;
        private final RequestContext context;

        HarnessContext(State state, RequestContext context) {
            this.state = state;
            this.context = context;
        }

        @Override
        public State state() {
            return state;
        }

        @Override
        public RequestContext context() {
            return context;
        }
    }
}
