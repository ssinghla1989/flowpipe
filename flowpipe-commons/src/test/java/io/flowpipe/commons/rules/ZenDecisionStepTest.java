package io.flowpipe.commons.rules;

import io.flowpipe.api.Failure;
import io.flowpipe.api.Result;
import io.flowpipe.api.RetryPolicy;
import io.flowpipe.api.Success;
import io.flowpipe.engine.Pipeline;
import io.flowpipe.state.RequestContext;
import io.gorules.zen_engine.JsonBuffer;
import io.gorules.zen_engine.ZenEngine;
import io.gorules.zen_engine.ZenException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ZenDecisionStepTest {

    private static ZenEngine engine;

    @BeforeAll
    static void createEngine() {
        engine = new ZenEngine(null, null);
    }

    @AfterAll
    static void closeEngine() {
        engine.close();
    }

    // --- descriptor ---

    @Test
    void hasCorrectDescriptor() {
        var step = ZenDecisionStep.fromClasspath("rules.eligibility", "/rules/eligibility.json", engine);
        assertThat(step.describe().id()).isEqualTo("rules.eligibility");
        assertThat(step.describe().inputType()).isEqualTo(JsonBuffer.class);
        assertThat(step.describe().outputType()).isEqualTo(JsonBuffer.class);
    }

    // --- construction ---

    @Test
    void throwsAtConstructionIfClasspathResourceMissing() {
        assertThatThrownBy(() ->
            ZenDecisionStep.fromClasspath("rules.x", "/rules/nonexistent.json", engine))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("not found on classpath");
    }

    @Test
    void throwsAtConstructionIfDecisionContentInvalid() {
        assertThatThrownBy(() ->
            ZenDecisionStep.fromContent("rules.x", "not-valid-json", engine))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void loadsFromContent() {
        String json = """
                {
                  "nodes": [
                    {"id": "i", "type": "inputNode",  "position": {"x": 0, "y": 0}, "name": "In"},
                    {"id": "o", "type": "outputNode", "position": {"x": 300, "y": 0}, "name": "Out"}
                  ],
                  "edges": []
                }
                """;
        var step = ZenDecisionStep.fromContent("rules.passthrough", json, engine);
        assertThat(step.describe().id()).isEqualTo("rules.passthrough");
    }

    // --- execution via pipeline ---

    @Test
    void premiumScoreProducesCorrectOutput() {
        var step = ZenDecisionStep.fromClasspath("rules.eligibility", "/rules/eligibility.json", engine);
        var pipeline = Pipeline.builder(JsonBuffer.class).then(step).build();

        Result<JsonBuffer> result = pipeline.execute(new JsonBuffer("{\"score\": 750}"), RequestContext.empty());

        assertThat(result).isInstanceOf(Success.class);
        String output = ((Success<JsonBuffer>) result).value().toString();
        assertThat(output).contains("\"eligible\":true");
        assertThat(output).contains("\"tier\":\"premium\"");
    }

    @Test
    void standardScoreProducesCorrectOutput() {
        var step = ZenDecisionStep.fromClasspath("rules.eligibility", "/rules/eligibility.json", engine);
        var pipeline = Pipeline.builder(JsonBuffer.class).then(step).build();

        Result<JsonBuffer> result = pipeline.execute(new JsonBuffer("{\"score\": 650}"), RequestContext.empty());

        assertThat(result).isInstanceOf(Success.class);
        String output = ((Success<JsonBuffer>) result).value().toString();
        assertThat(output).contains("\"eligible\":true");
        assertThat(output).contains("\"tier\":\"standard\"");
    }

    @Test
    void rejectedScoreProducesCorrectOutput() {
        var step = ZenDecisionStep.fromClasspath("rules.eligibility", "/rules/eligibility.json", engine);
        var pipeline = Pipeline.builder(JsonBuffer.class).then(step).build();

        Result<JsonBuffer> result = pipeline.execute(new JsonBuffer("{\"score\": 500}"), RequestContext.empty());

        assertThat(result).isInstanceOf(Success.class);
        String output = ((Success<JsonBuffer>) result).value().toString();
        assertThat(output).contains("\"eligible\":false");
        assertThat(output).contains("\"tier\":\"rejected\"");
    }

    @Test
    void malformedContextFails() {
        var step = ZenDecisionStep.fromClasspath("rules.eligibility", "/rules/eligibility.json", engine);
        var pipeline = Pipeline.builder(JsonBuffer.class).then(step).build();

        Result<JsonBuffer> result = pipeline.execute(new JsonBuffer("{not-json}"), RequestContext.empty());

        assertThat(result).isInstanceOf(Failure.class);
        assertThat(((Failure<JsonBuffer>) result).cause()).isInstanceOf(ZenException.class);
    }

    // --- policy overrides (inherited from Step) ---

    @Test
    void withRetryReturnsDifferentInstanceWithPolicyApplied() {
        var step = ZenDecisionStep.fromClasspath("rules.eligibility", "/rules/eligibility.json", engine);
        var withRetry = step.withRetry(RetryPolicy.fixed(2, 0));

        assertThat(withRetry).isNotSameAs(step);
        assertThat(withRetry.describe().retryPolicy().maxAttempts()).isEqualTo(2);
        assertThat(step.describe().retryPolicy().maxAttempts()).isEqualTo(1);
    }
}
