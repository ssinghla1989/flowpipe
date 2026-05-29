package io.flowpipe.commons.validation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import io.flowpipe.api.Step;
import io.flowpipe.api.StepContext;
import io.flowpipe.api.StepDescriptor;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Set;

/**
 * Validates a JSON string against a JSON Schema (Draft-07).
 *
 * <p>Use as the first step in a pipeline to validate incoming request bodies, or after an API
 * call step to assert the response matches a known contract. On success the original JSON string
 * passes through unchanged; on failure a {@link JsonSchemaValidationException} is thrown carrying
 * every violation.
 *
 * <pre>{@code
 * // Load schema from the classpath (recommended — fails fast at construction if missing)
 * var validateRequest = new JsonSchemaValidationStep("validate.order-request", "/schemas/order-request.json");
 *
 * // Load schema from a string (e.g. fetched from a schema registry at startup)
 * var validateResponse = JsonSchemaValidationStep.fromContent("validate.order-response", schemaJson);
 *
 * // Override policies at the wiring site — inherited from Step, no per-step boilerplate needed
 * Pipeline<String, OrderResponse> pipeline = Pipeline.builder(String.class, OrderResponse.class)
 *     .then(validateRequest.withTimeout(TimeoutPolicy.ofMillis(50)))
 *     .then(parseStep)
 *     .build();
 * }</pre>
 */
public final class JsonSchemaValidationStep implements Step<String, String> {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final JsonSchemaFactory FACTORY =
            JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7);

    private final JsonSchema schema;
    private final StepDescriptor<String, String> descriptor;

    /**
     * Loads the JSON Schema from a classpath resource. Fails immediately at construction if the
     * resource is missing or the schema is invalid — not at first request.
     *
     * @param id                step id, unique within the pipeline
     * @param classpathResource absolute classpath path, e.g. {@code "/schemas/order.json"}
     */
    public JsonSchemaValidationStep(String id, String classpathResource) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(classpathResource, "classpathResource");
        try (InputStream is = JsonSchemaValidationStep.class.getResourceAsStream(classpathResource)) {
            if (is == null) {
                throw new IllegalArgumentException("Schema resource not found on classpath: " + classpathResource);
            }
            this.schema = FACTORY.getSchema(is);
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to load schema from: " + classpathResource, e);
        }
        this.descriptor = StepDescriptor.builder(id, String.class, String.class).build();
    }

    /**
     * Loads the JSON Schema from a raw JSON string (e.g. retrieved from a schema registry).
     *
     * @param id         step id, unique within the pipeline
     * @param schemaJson the full JSON Schema document as a string
     */
    public static JsonSchemaValidationStep fromContent(String id, String schemaJson) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(schemaJson, "schemaJson");
        byte[] bytes = schemaJson.getBytes(StandardCharsets.UTF_8);
        try (InputStream is = new ByteArrayInputStream(bytes)) {
            JsonSchema schema = FACTORY.getSchema(is);
            return new JsonSchemaValidationStep(id, schema);
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid JSON Schema content", e);
        }
    }

    private JsonSchemaValidationStep(String id, JsonSchema schema) {
        this.schema = schema;
        this.descriptor = StepDescriptor.builder(id, String.class, String.class).build();
    }

    @Override
    public StepDescriptor<String, String> describe() {
        return descriptor;
    }

    @Override
    public String execute(String json, StepContext ctx) throws Exception {
        var node = MAPPER.readTree(json);
        Set<ValidationMessage> violations = schema.validate(node);
        if (!violations.isEmpty()) {
            throw new JsonSchemaValidationException(descriptor.id(), violations);
        }
        return json;
    }
}
