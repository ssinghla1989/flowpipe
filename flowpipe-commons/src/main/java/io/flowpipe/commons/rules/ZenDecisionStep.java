package io.flowpipe.commons.rules;

import io.flowpipe.api.Step;
import io.flowpipe.api.StepContext;
import io.flowpipe.api.StepDescriptor;
import io.gorules.zen_engine.JsonBuffer;
import io.gorules.zen_engine.ZenDecision;
import io.gorules.zen_engine.ZenDecisionInterface;
import io.gorules.zen_engine.ZenEngine;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

/**
 * Evaluates a gorules zen decision graph against a JSON context.
 *
 * <p>Input is the evaluation context as a {@link JsonBuffer}; output is the decision result
 * as a {@link JsonBuffer}. The decision is loaded once at construction time — not per request.
 * Use {@link JsonBuffer#JsonBuffer(String)} and {@link JsonBuffer#toString()} to convert
 * between {@code JsonBuffer} and JSON strings at the pipeline wiring site.
 *
 * <pre>{@code
 * // Shared engine — create once at startup, close on shutdown
 * ZenEngine engine = new ZenEngine(null, null);
 *
 * // Load from classpath (fails at startup if missing — not at first request)
 * var eligibilityRule = ZenDecisionStep.fromClasspath(
 *     "rules.eligibility", "/rules/eligibility.json", engine);
 *
 * Pipeline<JsonBuffer, JsonBuffer> pipeline = Pipeline.builder(JsonBuffer.class)
 *     .then(eligibilityRule)
 *     .build();
 *
 * // At call time: wrap your context JSON going in, read result JSON back out
 * var result = pipeline.execute(new JsonBuffer(contextJson), RequestContext.empty());
 * }</pre>
 */
public final class ZenDecisionStep implements Step<JsonBuffer, JsonBuffer> {

    private final ZenDecisionInterface decision;
    private final StepDescriptor<JsonBuffer, JsonBuffer> descriptor;

    /**
     * Wraps an already-loaded {@link ZenDecisionInterface}.
     */
    public static ZenDecisionStep of(String id, ZenDecisionInterface decision) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(decision, "decision");
        return new ZenDecisionStep(id, decision);
    }

    /**
     * Loads the decision from a classpath resource. Fails immediately at construction if the
     * resource is missing or invalid — not at first request.
     *
     * @param id                step id, unique within the pipeline
     * @param classpathResource absolute classpath path, e.g. {@code "/rules/eligibility.json"}
     * @param engine            the {@link ZenEngine} used to parse the decision content
     */
    public static ZenDecisionStep fromClasspath(String id, String classpathResource, ZenEngine engine) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(classpathResource, "classpathResource");
        Objects.requireNonNull(engine, "engine");
        try (InputStream is = ZenDecisionStep.class.getResourceAsStream(classpathResource)) {
            if (is == null) {
                throw new IllegalArgumentException("Decision resource not found on classpath: " + classpathResource);
            }
            String content = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            return fromContent(id, content, engine);
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to load decision from: " + classpathResource, e);
        }
    }

    /**
     * Loads the decision from a raw JSON string (e.g. retrieved from a rules registry at startup).
     *
     * @param id          step id, unique within the pipeline
     * @param decisionJson the full gorules decision graph JSON as a string
     * @param engine       the {@link ZenEngine} used to parse the decision content
     */
    public static ZenDecisionStep fromContent(String id, String decisionJson, ZenEngine engine) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(decisionJson, "decisionJson");
        Objects.requireNonNull(engine, "engine");
        try {
            ZenDecision decision = engine.createDecision(new JsonBuffer(decisionJson));
            return new ZenDecisionStep(id, decision);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid decision content", e);
        }
    }

    private ZenDecisionStep(String id, ZenDecisionInterface decision) {
        this.decision = decision;
        this.descriptor = StepDescriptor.builder(id, JsonBuffer.class, JsonBuffer.class).build();
    }

    @Override
    public StepDescriptor<JsonBuffer, JsonBuffer> describe() {
        return descriptor;
    }

    @Override
    public JsonBuffer execute(JsonBuffer context, StepContext ctx) throws Exception {
        try {
            return decision.evaluate(context, null).get().result();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            throw (cause instanceof Exception ex) ? ex : new RuntimeException(cause);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        }
    }
}
