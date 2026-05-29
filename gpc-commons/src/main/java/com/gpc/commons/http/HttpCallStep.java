package com.gpc.commons.http;

import io.flowpipe.api.Step;
import io.flowpipe.api.StepContext;
import io.flowpipe.api.StepDescriptor;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Objects;

/**
 * Makes a synchronous outbound HTTP call using the JDK {@link HttpClient} (no external dependency).
 *
 * <p>Input is a fully-built {@link HttpRequest}; output is the response body as a {@code String}.
 * Any non-2xx response is surfaced as {@link HttpCallException} carrying the status code and
 * raw body — FlowPipe wraps this in a {@code Failure} and triggers retry/circuit-breaker policies
 * as configured at the wiring site.
 *
 * <pre>{@code
 * var httpCall = new HttpCallStep("call.inventory-api");
 *
 * // Custom client — e.g. with redirect policy or custom SSL context
 * var httpCall = new HttpCallStep("call.inventory-api",
 *     HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NORMAL).build());
 *
 * // Wiring — caller builds the HttpRequest in a preceding inline step
 * Pipeline<OrderRequest, String> pipeline = Pipeline.builder(OrderRequest.class, String.class)
 *     .then(Step.of("build.request", OrderRequest.class, HttpRequest.class,
 *         (order, ctx) -> HttpRequest.newBuilder()
 *             .uri(URI.create("https://api.example.com/inventory/" + order.sku()))
 *             .GET().build()))
 *     .then(httpCall.withRetry(RetryPolicy.builder().maxAttempts(3).build()))
 *     .then(validateResponseStep)
 *     .build();
 * }</pre>
 */
public final class HttpCallStep implements Step<HttpRequest, String> {

    private static final HttpClient DEFAULT_CLIENT = HttpClient.newHttpClient();

    private final HttpClient client;
    private final StepDescriptor<HttpRequest, String> descriptor;

    public HttpCallStep(String id) {
        this(id, DEFAULT_CLIENT);
    }

    public HttpCallStep(String id, HttpClient client) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(client, "client");
        this.client = client;
        this.descriptor = StepDescriptor.builder(id, HttpRequest.class, String.class).build();
    }

    @Override
    public StepDescriptor<HttpRequest, String> describe() {
        return descriptor;
    }

    @Override
    public String execute(HttpRequest request, StepContext ctx) throws Exception {
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new HttpCallException(response.statusCode(), response.body());
        }
        return response.body();
    }
}
