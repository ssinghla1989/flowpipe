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
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Validates a JSON string against a JSON Schema.
 *
 * <p>Use as the first step in a pipeline to validate incoming request bodies, or after an API
 * call step to assert the response matches a known contract. On success the original JSON string
 * passes through unchanged; on failure a {@link JsonSchemaValidationException} is thrown carrying
 * every violation.
 *
 * <h2>Spec versions</h2>
 * <p>Defaults to {@link SpecVersion.VersionFlag#V7 Draft-07} for backward compatibility. Pass an
 * explicit {@link SpecVersion.VersionFlag} to use {@link SpecVersion.VersionFlag#V201909
 * Draft-2019-09} or {@link SpecVersion.VersionFlag#V202012 Draft-2020-12}. All non-deprecated
 * Draft-* versions supported by the underlying networknt validator are available.
 *
 * <pre>{@code
 * // Default (Draft-07)
 * var validateRequest = new JsonSchemaValidationStep("validate.order-request", "/schemas/order-request.json");
 *
 * // Explicit Draft-2020-12
 * var validateRequest = new JsonSchemaValidationStep(
 *     "validate.order-request", "/schemas/order-request.json", SpecVersion.VersionFlag.V202012);
 *
 * // Override policies at the wiring site — inherited from Step, no per-step boilerplate needed
 * Pipeline<String, String> pipeline = Pipeline.builder(String.class)
 *     .then(validateRequest.withTimeout(TimeoutPolicy.ofMillis(50)))
 *     .then(parseStep)
 *     .build();
 * }</pre>
 */
public final class JsonSchemaValidationStep implements Step<String, String> {

    public static final SpecVersion.VersionFlag DEFAULT_SPEC_VERSION = SpecVersion.VersionFlag.V7;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // One factory per spec version; constructed lazily on first use and reused thereafter.
    // EnumMap is read-mostly; synchronized writes are fine since instances live for app lifetime.
    private static final Map<SpecVersion.VersionFlag, JsonSchemaFactory> FACTORIES =
            new EnumMap<>(SpecVersion.VersionFlag.class);

    private static JsonSchemaFactory factoryFor(SpecVersion.VersionFlag version) {
        synchronized (FACTORIES) {
            return FACTORIES.computeIfAbsent(version, JsonSchemaFactory::getInstance);
        }
    }

    private final JsonSchema schema;
    private final StepDescriptor<String, String> descriptor;

    /**
     * Loads a Draft-07 JSON Schema from a classpath resource.
     */
    public JsonSchemaValidationStep(String id, String classpathResource) {
        this(id, classpathResource, DEFAULT_SPEC_VERSION);
    }

    /**
     * Loads a JSON Schema from a classpath resource using the given spec version. Fails
     * immediately at construction if the resource is missing or the schema is invalid — not at
     * first request.
     *
     * @param id                step id, unique within the pipeline
     * @param classpathResource absolute classpath path, e.g. {@code "/schemas/order.json"}
     * @param specVersion       JSON Schema draft version to parse against (Draft-07, 2019-09, etc.)
     */
    public JsonSchemaValidationStep(String id, String classpathResource,
                                    SpecVersion.VersionFlag specVersion) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(classpathResource, "classpathResource");
        Objects.requireNonNull(specVersion, "specVersion");
        JsonSchemaFactory factory = factoryFor(specVersion);
        try (InputStream is = JsonSchemaValidationStep.class.getResourceAsStream(classpathResource)) {
            if (is == null) {
                throw new IllegalArgumentException("Schema resource not found on classpath: " + classpathResource);
            }
            this.schema = factory.getSchema(is);
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to load schema from: " + classpathResource, e);
        }
        this.descriptor = StepDescriptor.builder(id, String.class, String.class).build();
    }

    /**
     * Loads a Draft-07 JSON Schema from a raw JSON string.
     */
    public static JsonSchemaValidationStep fromContent(String id, String schemaJson) {
        return fromContent(id, schemaJson, DEFAULT_SPEC_VERSION);
    }

    /**
     * Loads a JSON Schema from a raw JSON string using the given spec version
     * (e.g. retrieved from a schema registry).
     *
     * @param id          step id, unique within the pipeline
     * @param schemaJson  the full JSON Schema document as a string
     * @param specVersion JSON Schema draft version to parse against
     */
    public static JsonSchemaValidationStep fromContent(String id, String schemaJson,
                                                       SpecVersion.VersionFlag specVersion) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(schemaJson, "schemaJson");
        Objects.requireNonNull(specVersion, "specVersion");
        JsonSchemaFactory factory = factoryFor(specVersion);
        byte[] bytes = schemaJson.getBytes(StandardCharsets.UTF_8);
        try (InputStream is = new ByteArrayInputStream(bytes)) {
            JsonSchema schema = factory.getSchema(is);
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
