package io.flowpipe.api;

import io.flowpipe.state.StateKey;
import io.flowpipe.validation.NoOpValidator;
import io.flowpipe.validation.Validator;

import java.util.Objects;

/**
 * Fluent builder for defining a {@link Step} with all configuration — body, policies, and
 * validators — in one expression.
 *
 * <p>Obtain a builder via {@link Step#builder(String, Class, Class)}:
 * <pre>{@code
 * Step<OrderRequest, ApiResponse> step = Step.builder("call.api", OrderRequest.class, ApiResponse.class)
 *     .execute((req, ctx) -> client.call(req))
 *     .withRetry(RetryPolicy.fixed(3, 100))
 *     .withTimeout(TimeoutPolicy.ofMillis(500))
 *     .build();
 * }</pre>
 *
 * <p>The body supplied to {@link #execute} may throw any exception — checked or unchecked — without
 * wrapping, since {@link Body#apply} declares {@code throws Exception}.
 *
 * <p>Calling {@link #build()} without calling {@link #execute} first throws
 * {@link IllegalStateException}.
 */
public final class StepBuilder<I, O> {

    /**
     * Functional interface for the step body. Declares {@code throws Exception} so checked
     * exceptions may be thrown from lambda bodies without wrapping.
     */
    @FunctionalInterface
    public interface Body<I, O> {
        O apply(I input, StepContext ctx) throws Exception;
    }

    private final String id;
    private final Class<I> inputType;
    private final Class<O> outputType;
    private Body<I, O> body;
    private RetryPolicy retryPolicy = RetryPolicy.none();
    private TimeoutPolicy timeoutPolicy = TimeoutPolicy.none();
    private CircuitBreakerPolicy circuitBreakerPolicy = null;
    private StateKey<O> outputKey = null;
    private Validator<I> inputValidator = NoOpValidator.instance();
    private Validator<O> outputValidator = NoOpValidator.instance();

    StepBuilder(String id, Class<I> inputType, Class<O> outputType) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(inputType, "inputType");
        Objects.requireNonNull(outputType, "outputType");
        if (id.isEmpty()) throw new IllegalArgumentException("id must not be empty");
        this.id = id;
        this.inputType = inputType;
        this.outputType = outputType;
    }

    public StepBuilder<I, O> execute(Body<I, O> body) {
        this.body = Objects.requireNonNull(body, "body");
        return this;
    }

    public StepBuilder<I, O> withRetry(RetryPolicy policy) {
        this.retryPolicy = Objects.requireNonNull(policy, "policy");
        return this;
    }

    public StepBuilder<I, O> withTimeout(TimeoutPolicy policy) {
        this.timeoutPolicy = Objects.requireNonNull(policy, "policy");
        return this;
    }

    public StepBuilder<I, O> withCircuitBreaker(CircuitBreakerPolicy policy) {
        this.circuitBreakerPolicy = Objects.requireNonNull(policy, "policy");
        return this;
    }

    public StepBuilder<I, O> withOutputKey(StateKey<O> key) {
        this.outputKey = Objects.requireNonNull(key, "key");
        return this;
    }

    public StepBuilder<I, O> withInputValidator(Validator<I> validator) {
        this.inputValidator = Objects.requireNonNull(validator, "validator");
        return this;
    }

    public StepBuilder<I, O> withOutputValidator(Validator<O> validator) {
        this.outputValidator = Objects.requireNonNull(validator, "validator");
        return this;
    }

    public Step<I, O> build() {
        if (body == null) {
            throw new IllegalStateException(
                "StepBuilder for '" + id + "' requires .execute(body) before .build()");
        }
        StepDescriptor<I, O> descriptor = new StepDescriptor<>(
            id, inputType, outputType, inputValidator, outputValidator,
            retryPolicy, timeoutPolicy, circuitBreakerPolicy, outputKey);
        Body<I, O> captured = body;
        return new Step<>() {
            @Override public StepDescriptor<I, O> describe() { return descriptor; }
            @Override public O execute(I input, StepContext ctx) throws Exception { return captured.apply(input, ctx); }
        };
    }
}
