package io.flowpipe.commons.rules;

import io.flowpipe.api.Result;
import io.flowpipe.api.Success;
import io.flowpipe.engine.Pipeline;
import io.flowpipe.state.RequestContext;
import io.gorules.zen_engine.JsonBuffer;
import io.gorules.zen_engine.ZenEngine;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Demonstrates four Zen rule types and four pipeline patterns using ZenDecisionStep.
 *
 * <h3>Rule types (one JSON file each under src/test/resources/rules/)</h3>
 * <ul>
 *   <li>{@code eligibility.json} — decision table, hitPolicy=first, numeric input → tier string output</li>
 *   <li>{@code risk-score.json} — expressionNode, formula-computed output field</li>
 *   <li>{@code fees.json}       — decision table, hitPolicy=collect, all matching rows fire</li>
 *   <li>{@code benefits.json}   — decision table, hitPolicy=first, string input → benefits output</li>
 * </ul>
 *
 * <h3>Pipeline patterns</h3>
 * <ol>
 *   <li>Single step — one rule in a pipeline</li>
 *   <li>Collect output — all matching rows returned as a JSON array</li>
 *   <li>Sequential chain — rule A output feeds rule B (eligibility tier → benefits lookup)</li>
 *   <li>Parallel evaluation — two independent rules on the same input, combined with a typed combiner</li>
 * </ol>
 */
class ZenDecisionExamplesTest {

    // Create one engine at class load; reuse across all tests. Close after all tests run.
    private static ZenEngine engine;

    @BeforeAll
    static void createEngine() {
        engine = new ZenEngine(null, null);
    }

    @AfterAll
    static void closeEngine() {
        engine.close();
    }

    // ── 1. expressionNode — formula-computed risk score ───────────────────────
    //
    // Use expressionNode when the output is a derived/computed field rather than
    // a lookup. The expression is evaluated in the Zen expression language (similar
    // to JavaScript arithmetic). All input fields are in scope by name.
    //
    // risk-score.json graph:
    //   inputNode → expressionNode (riskScore = creditScore / 10 + (income / loanAmount) * 100)
    //             → outputNode
    //
    // Output shape: {"riskScore": <number>}

    @Test
    void expressionNode_computesRiskScore() {
        var step = ZenDecisionStep.fromClasspath("risk.score", "/rules/risk-score.json", engine);
        Pipeline<JsonBuffer, JsonBuffer> pipeline = Pipeline.builder(JsonBuffer.class)
            .then(step)
            .build();

        // creditScore=700, income=60 000, loanAmount=200 000
        // riskScore = 700/10 + (60000/200000)*100 = 70 + 30 = 100
        Result<JsonBuffer> result = pipeline.execute(
            new JsonBuffer("{\"creditScore\":700,\"income\":60000,\"loanAmount\":200000}"),
            RequestContext.empty());

        assertThat(result).isInstanceOf(Success.class);
        String output = ((Success<JsonBuffer>) result).value().toString();
        assertThat(output).isEqualTo("{\"riskScore\":100}");
    }

    @Test
    void expressionNode_higherScoreHigherRisk() {
        var step = ZenDecisionStep.fromClasspath("risk.score", "/rules/risk-score.json", engine);
        Pipeline<JsonBuffer, JsonBuffer> pipeline = Pipeline.builder(JsonBuffer.class)
            .then(step)
            .build();

        // creditScore=800, income=40 000, loanAmount=400 000
        // riskScore = 800/10 + (40000/400000)*100 = 80 + 10 = 90
        Result<JsonBuffer> result = pipeline.execute(
            new JsonBuffer("{\"creditScore\":800,\"income\":40000,\"loanAmount\":400000}"),
            RequestContext.empty());

        assertThat(((Success<JsonBuffer>) result).value().toString()).isEqualTo("{\"riskScore\":90}");
    }

    // ── 2. decisionTable / hitPolicy=collect — stacking fees ─────────────────
    //
    // hitPolicy "collect" fires EVERY matching row, not just the first.
    // Use it when multiple rules can apply at the same time: fees, tags, applicable
    // policies, discount stacking.
    //
    // fees.json rules (empty condition = always matches):
    //   r1  (always)          → fee=10, label="processing"
    //   r2  (amount > 10000)  → fee=50, label="high_value"
    //   r3  (priority=express)→ fee=25, label="express"
    //
    // Output shape: array of matched-row objects
    //   [{fee:10, label:"processing"}, {fee:50, label:"high_value"}, ...]

    @Test
    void collect_allThreeFeesWhenHighValueAndExpress() {
        var step = ZenDecisionStep.fromClasspath("rules.fees", "/rules/fees.json", engine);
        Pipeline<JsonBuffer, JsonBuffer> pipeline = Pipeline.builder(JsonBuffer.class)
            .then(step)
            .build();

        // amount=15 000 matches r1 (always) and r2 (> 10000)
        // priority=express matches r1 (always) and r3
        // → all three rules fire
        Result<JsonBuffer> result = pipeline.execute(
            new JsonBuffer("{\"amount\":15000,\"priority\":\"express\"}"),
            RequestContext.empty());

        assertThat(result).isInstanceOf(Success.class);
        String output = ((Success<JsonBuffer>) result).value().toString();
        // collect returns a JSON array — one object per matched row
        assertThat(output).startsWith("[");
        assertThat(output).contains("\"label\":\"processing\"");
        assertThat(output).contains("\"label\":\"high_value\"");
        assertThat(output).contains("\"label\":\"express\"");
    }

    @Test
    void collect_onlyProcessingFeeForSmallStandardOrder() {
        var step = ZenDecisionStep.fromClasspath("rules.fees", "/rules/fees.json", engine);
        Pipeline<JsonBuffer, JsonBuffer> pipeline = Pipeline.builder(JsonBuffer.class)
            .then(step)
            .build();

        // amount=5 000 → only r1 fires (below 10 000, priority not express)
        Result<JsonBuffer> result = pipeline.execute(
            new JsonBuffer("{\"amount\":5000,\"priority\":\"standard\"}"),
            RequestContext.empty());

        assertThat(result).isInstanceOf(Success.class);
        String output = ((Success<JsonBuffer>) result).value().toString();
        assertThat(output).startsWith("[");
        assertThat(output).contains("\"label\":\"processing\"");
        assertThat(output).contains("\"fee\":10");
        // Only one rule fires — no high_value or express entries
        assertThat(output).doesNotContain("high_value");
        assertThat(output).doesNotContain("express");
    }

    // ── 3. Sequential chain — output of rule A feeds rule B ──────────────────
    //
    // Chain two ZenDecisionStep instances with .then(). The output JsonBuffer of the
    // first step becomes the input JsonBuffer of the second. Design your rules so each
    // step's output contains the fields the next step needs.
    //
    // Chain used here:
    //   eligibility.json  {score}              → {eligible, tier}
    //   benefits.json     {eligible, tier}     → {benefit, feeDiscount}
    //
    // benefits.json matches on the string field "tier" using first-hit policy.

    @Test
    void sequentialChain_premiumScoreGetsPrioritySupport() {
        var eligibilityStep = ZenDecisionStep.fromClasspath(
            "rules.eligibility", "/rules/eligibility.json", engine);
        var benefitsStep = ZenDecisionStep.fromClasspath(
            "rules.benefits", "/rules/benefits.json", engine);

        Pipeline<JsonBuffer, JsonBuffer> pipeline = Pipeline.builder(JsonBuffer.class)
            .then(eligibilityStep)   // {score:750} → {eligible:true, tier:"premium"}
            .then(benefitsStep)      // {eligible:true, tier:"premium"} → {benefit:..., feeDiscount:...}
            .build();

        Result<JsonBuffer> result = pipeline.execute(
            new JsonBuffer("{\"score\":750}"),
            RequestContext.empty());

        assertThat(result).isInstanceOf(Success.class);
        String output = ((Success<JsonBuffer>) result).value().toString();
        assertThat(output).contains("\"benefit\":\"priority_support\"");
        assertThat(output).contains("\"feeDiscount\":100");
    }

    @Test
    void sequentialChain_standardScoreGetsStandardSupport() {
        var eligibilityStep = ZenDecisionStep.fromClasspath(
            "rules.eligibility", "/rules/eligibility.json", engine);
        var benefitsStep = ZenDecisionStep.fromClasspath(
            "rules.benefits", "/rules/benefits.json", engine);

        Pipeline<JsonBuffer, JsonBuffer> pipeline = Pipeline.builder(JsonBuffer.class)
            .then(eligibilityStep)   // {score:650} → {eligible:true, tier:"standard"}
            .then(benefitsStep)      // {tier:"standard"} → {benefit:"standard_support", feeDiscount:0}
            .build();

        Result<JsonBuffer> result = pipeline.execute(
            new JsonBuffer("{\"score\":650}"),
            RequestContext.empty());

        assertThat(result).isInstanceOf(Success.class);
        String output = ((Success<JsonBuffer>) result).value().toString();
        assertThat(output).contains("\"benefit\":\"standard_support\"");
        assertThat(output).contains("\"feeDiscount\":0");
    }

    @Test
    void sequentialChain_rejectedScoreGetsNoDiscount() {
        var eligibilityStep = ZenDecisionStep.fromClasspath(
            "rules.eligibility", "/rules/eligibility.json", engine);
        var benefitsStep = ZenDecisionStep.fromClasspath(
            "rules.benefits", "/rules/benefits.json", engine);

        Pipeline<JsonBuffer, JsonBuffer> pipeline = Pipeline.builder(JsonBuffer.class)
            .then(eligibilityStep)   // {score:500} → {eligible:false, tier:"rejected"}
            .then(benefitsStep)      // {tier:"rejected"} → {benefit:"none", feeDiscount:0}
            .build();

        Result<JsonBuffer> result = pipeline.execute(
            new JsonBuffer("{\"score\":500}"),
            RequestContext.empty());

        assertThat(result).isInstanceOf(Success.class);
        String output = ((Success<JsonBuffer>) result).value().toString();
        assertThat(output).contains("\"benefit\":\"none\"");
    }

    // ── 4. Parallel evaluation — two independent rules on the same input ──────
    //
    // Run two ZenDecisionStep instances concurrently with parallel2. Both receive the
    // same input JsonBuffer. Use a typed combiner to merge their outputs.
    //
    // When to use: rules are independent (no shared mutable state, no ordering
    // dependency) and latency matters more than throughput.
    //
    // Rules used:
    //   eligibility  uses: score               → {eligible, tier}
    //   risk-score   uses: creditScore, income, loanAmount → {riskScore}
    // Both fields are present in the input — each rule reads only what it needs.

    @Test
    void parallelRules_eligibilityAndRiskScoreSimultaneously() {
        var eligibilityStep = ZenDecisionStep.fromClasspath(
            "rules.eligibility", "/rules/eligibility.json", engine);
        var riskStep = ZenDecisionStep.fromClasspath(
            "risk.score", "/rules/risk-score.json", engine);

        // parallel2 combiner receives the two rule results and merges them into
        // a final approval decision string. Class<R> (String.class) is the first arg.
        Pipeline<JsonBuffer, String> pipeline = Pipeline.builder(JsonBuffer.class)
            .parallel2(
                String.class,
                (eligResult, riskResult) -> {
                    // eligResult = {"eligible":true,"tier":"premium"}
                    // riskResult = {"riskScore":100}
                    boolean eligible = eligResult.toString().contains("\"eligible\":true");
                    return "{\"approved\":" + eligible
                        + ",\"eligibility\":" + eligResult
                        + ",\"risk\":" + riskResult + "}";
                },
                eligibilityStep,
                riskStep)
            .build();

        // Input has all fields needed by both rules
        Result<String> result = pipeline.execute(
            new JsonBuffer("{\"score\":750,\"creditScore\":700,\"income\":60000,\"loanAmount\":200000}"),
            RequestContext.empty());

        assertThat(result).isInstanceOf(Success.class);
        String output = ((Success<String>) result).value();
        assertThat(output).contains("\"approved\":true");
        assertThat(output).contains("\"tier\":\"premium\"");
        assertThat(output).contains("\"riskScore\":100");
    }

    @Test
    void parallelRules_rejectedApplicantIsNotApproved() {
        var eligibilityStep = ZenDecisionStep.fromClasspath(
            "rules.eligibility", "/rules/eligibility.json", engine);
        var riskStep = ZenDecisionStep.fromClasspath(
            "risk.score", "/rules/risk-score.json", engine);

        Pipeline<JsonBuffer, String> pipeline = Pipeline.builder(JsonBuffer.class)
            .parallel2(
                String.class,
                (eligResult, riskResult) -> {
                    boolean eligible = eligResult.toString().contains("\"eligible\":true");
                    return "{\"approved\":" + eligible
                        + ",\"eligibility\":" + eligResult
                        + ",\"risk\":" + riskResult + "}";
                },
                eligibilityStep,
                riskStep)
            .build();

        // score=500 → rejected; creditScore=400 → riskScore = 40 + 50 = 90
        Result<String> result = pipeline.execute(
            new JsonBuffer("{\"score\":500,\"creditScore\":400,\"income\":50000,\"loanAmount\":100000}"),
            RequestContext.empty());

        assertThat(result).isInstanceOf(Success.class);
        String output = ((Success<String>) result).value();
        assertThat(output).contains("\"approved\":false");
        assertThat(output).contains("\"tier\":\"rejected\"");
    }
}
