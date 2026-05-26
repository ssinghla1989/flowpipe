package io.flowpipe.observability;

import io.flowpipe.state.RequestContext;

/**
 * SPI for integrating distributed tracing around every step execution.
 *
 * <p>Register an implementation via {@code PipelineBuilder.withTracing(SpanRecorder)}. The
 * framework calls {@link #startStep} before the first attempt of a step and
 * {@link #finishStep} after the final outcome — success, failure, or circuit-breaker trip.
 * Skipped steps (branch arms that did not run) also trigger both callbacks with
 * {@link StepOutcome#SKIPPED}.
 *
 * <p>This means no developer code in {@link io.flowpipe.api.PipelineLifecycle} is needed for
 * tracing; step-level spans are fully managed by the framework.
 *
 * <p>Implementations must be thread-safe when the pipeline uses parallel composition or
 * concurrent foreach. Exceptions thrown by either method are suppressed by the framework
 * and logged as warnings.
 */
public interface SpanRecorder {

    /**
     * Called before a step's first attempt begins.
     *
     * @param stepId  the id of the step being started
     * @param context the immutable request-scoped context (trace ids, tenant ids, etc.)
     * @return an opaque span token that will be passed back to {@link #finishStep}; may be null
     */
    Object startStep(String stepId, RequestContext context);

    /**
     * Called after the step completes — whether on first-attempt success, final retry failure,
     * circuit-breaker trip, or skip.
     *
     * @param span    the token returned by {@link #startStep}; null if startStep returned null
     * @param outcome the final outcome
     * @param cause   the terminal exception for {@link StepOutcome#FAILURE}; null otherwise
     */
    void finishStep(Object span, StepOutcome outcome, Throwable cause);
}
