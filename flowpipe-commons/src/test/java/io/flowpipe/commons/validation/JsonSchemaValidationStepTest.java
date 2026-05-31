package io.flowpipe.commons.validation;

import com.networknt.schema.SpecVersion;
import io.flowpipe.api.Failure;
import io.flowpipe.api.Result;
import io.flowpipe.api.Success;
import io.flowpipe.engine.Pipeline;
import io.flowpipe.state.RequestContext;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JsonSchemaValidationStepTest {

    private static final String VALID_ORDER = """
            {"orderId":"O-001","customerId":"C-42","items":[{"sku":"WIDGET","quantity":3}]}
            """;

    private static final String MISSING_FIELD = """
            {"orderId":"O-001","items":[{"sku":"WIDGET","quantity":3}]}
            """;

    private static final String NEGATIVE_QUANTITY = """
            {"orderId":"O-001","customerId":"C-42","items":[{"sku":"WIDGET","quantity":0}]}
            """;

    private static final String SCHEMA_JSON = """
            {
              "$schema": "http://json-schema.org/draft-07/schema#",
              "type": "object",
              "required": ["name"],
              "properties": {
                "name": { "type": "string" }
              }
            }
            """;

    // --- construction ---

    @Test
    void loadsSchemaFromClasspath() {
        var step = new JsonSchemaValidationStep("validate.order", "/schemas/order-request.json");
        assertThat(step.describe().id()).isEqualTo("validate.order");
        assertThat(step.describe().inputType()).isEqualTo(String.class);
        assertThat(step.describe().outputType()).isEqualTo(String.class);
    }

    @Test
    void throwsAtConstructionIfClasspathResourceMissing() {
        assertThatThrownBy(() -> new JsonSchemaValidationStep("s", "/schemas/nonexistent.json"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not found on classpath");
    }

    @Test
    void loadsSchemaFromContent() {
        var step = JsonSchemaValidationStep.fromContent("validate.name", SCHEMA_JSON);
        assertThat(step.describe().id()).isEqualTo("validate.name");
    }

    @Test
    void throwsAtConstructionIfSchemaContentInvalid() {
        assertThatThrownBy(() -> JsonSchemaValidationStep.fromContent("s", "not-json"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // --- execution via pipeline ---

    @Test
    void passesValidJsonThrough() {
        var step = new JsonSchemaValidationStep("validate.order", "/schemas/order-request.json");
        var pipeline = Pipeline.builder(String.class).then(step).build();

        Result<String> result = pipeline.execute(VALID_ORDER.strip(), RequestContext.empty());

        assertThat(result).isInstanceOf(Success.class);
        assertThat(((Success<String>) result).value()).isEqualTo(VALID_ORDER.strip());
    }

    @Test
    void failsWhenRequiredFieldMissing() {
        var step = new JsonSchemaValidationStep("validate.order", "/schemas/order-request.json");
        var pipeline = Pipeline.builder(String.class).then(step).build();

        Result<String> result = pipeline.execute(MISSING_FIELD.strip(), RequestContext.empty());

        assertThat(result).isInstanceOf(Failure.class);
        var failure = (Failure<String>) result;
        assertThat(failure.cause()).isInstanceOf(JsonSchemaValidationException.class);

        var ex = (JsonSchemaValidationException) failure.cause();
        assertThat(ex.stepId()).isEqualTo("validate.order");
        assertThat(ex.violations()).isNotEmpty();
        assertThat(ex.violations()).anyMatch(v -> v.getMessage().contains("customerId"));
    }

    @Test
    void failsWhenConstraintViolated() {
        var step = new JsonSchemaValidationStep("validate.order", "/schemas/order-request.json");
        var pipeline = Pipeline.builder(String.class).then(step).build();

        Result<String> result = pipeline.execute(NEGATIVE_QUANTITY.strip(), RequestContext.empty());

        assertThat(result).isInstanceOf(Failure.class);
        var ex = (JsonSchemaValidationException) ((Failure<String>) result).cause();
        assertThat(ex.violations()).anyMatch(v -> v.getMessage().contains("quantity"));
    }

    @Test
    void failsOnMalformedJson() {
        var step = new JsonSchemaValidationStep("validate.order", "/schemas/order-request.json");
        var pipeline = Pipeline.builder(String.class).then(step).build();

        Result<String> result = pipeline.execute("{not-json}", RequestContext.empty());

        assertThat(result).isInstanceOf(Failure.class);
        // Jackson parse error, not a schema violation
        assertThat(((Failure<String>) result).cause())
                .isNotInstanceOf(JsonSchemaValidationException.class);
    }

    // --- policy overrides via Step default methods (no per-step boilerplate) ---

    @Test
    void withRetryReturnsDifferentInstanceWithPolicyApplied() {
        var step = new JsonSchemaValidationStep("validate.order", "/schemas/order-request.json");
        var withRetry = step.withRetry(io.flowpipe.api.RetryPolicy.fixed(2, 0));

        assertThat(withRetry).isNotSameAs(step);
        assertThat(withRetry.describe().retryPolicy().maxAttempts()).isEqualTo(2);
        assertThat(step.describe().retryPolicy().maxAttempts()).isEqualTo(1);
    }

    @Test
    void withTimeoutReturnsDifferentInstanceWithPolicyApplied() {
        var step = new JsonSchemaValidationStep("validate.order", "/schemas/order-request.json");
        var withTimeout = step.withTimeout(io.flowpipe.api.TimeoutPolicy.ofMillis(100));

        assertThat(withTimeout).isNotSameAs(step);
        assertThat(withTimeout.describe().timeoutPolicy().timeoutMs()).isEqualTo(100);
    }

    @Test
    void policyOverridesAreChainable() {
        var step = new JsonSchemaValidationStep("validate.order", "/schemas/order-request.json")
                .withRetry(io.flowpipe.api.RetryPolicy.fixed(3, 0))
                .withTimeout(io.flowpipe.api.TimeoutPolicy.ofMillis(200));

        assertThat(step.describe().retryPolicy().maxAttempts()).isEqualTo(3);
        assertThat(step.describe().timeoutPolicy().timeoutMs()).isEqualTo(200);
    }

    // --- spec version ---

    private static final String DRAFT_2020_12_SCHEMA = """
            {
              "$schema": "https://json-schema.org/draft/2020-12/schema",
              "type": "object",
              "required": ["name"],
              "properties": { "name": { "type": "string" } }
            }
            """;

    @Test
    void loadsSchemaUsingDraft2020_12() {
        var step = JsonSchemaValidationStep.fromContent(
            "validate.name", DRAFT_2020_12_SCHEMA, SpecVersion.VersionFlag.V202012);
        var pipeline = Pipeline.builder(String.class).then(step).build();

        Result<String> ok = pipeline.execute("{\"name\":\"ada\"}", RequestContext.empty());
        assertThat(ok).isInstanceOf(Success.class);

        Result<String> bad = pipeline.execute("{}", RequestContext.empty());
        assertThat(bad).isInstanceOf(Failure.class);
    }

    @Test
    void defaultSpecVersionIsDraft07() {
        assertThat(JsonSchemaValidationStep.DEFAULT_SPEC_VERSION).isEqualTo(SpecVersion.VersionFlag.V7);
    }

    // --- usable as first step in request-validation pipeline ---

    @Test
    void usableAsFirstPipelineStepForRequestValidation() {
        var validateRequest = new JsonSchemaValidationStep("validate.request", "/schemas/order-request.json");
        var pipeline = Pipeline.builder(String.class)
                .then(validateRequest)
                .build();

        assertThat(pipeline.execute(VALID_ORDER.strip(), RequestContext.empty()))
                .isInstanceOf(Success.class);
        assertThat(pipeline.execute(MISSING_FIELD.strip(), RequestContext.empty()))
                .isInstanceOf(Failure.class);
    }
}
