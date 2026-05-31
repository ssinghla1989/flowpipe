package io.flowpipe.commons.http;

import io.flowpipe.api.Step;
import io.flowpipe.api.StepContext;
import io.flowpipe.api.StepDescriptor;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Objects;

/**
 * Makes a synchronous outbound HTTP call using the JDK {@link HttpClient} (no external dependency).
 *
 * <p>Input is a fully-built {@link HttpRequest}; output is the response body as a {@code String}.
 * Any non-2xx response is surfaced as {@link HttpCallException} carrying the status code and
 * raw body — FlowPipe wraps this in a {@code Failure} and triggers retry/circuit-breaker policies
 * as configured at the wiring site.
 *
 * <h2>Defaults</h2>
 * <ul>
 *   <li><b>Connect timeout:</b> {@value DEFAULT_CONNECT_TIMEOUT_SECONDS}s — applied to the shared
 *       default client. A hung TCP handshake will fail rather than block indefinitely.</li>
 *   <li><b>Request timeout:</b> {@value DEFAULT_REQUEST_TIMEOUT_SECONDS}s — applied per-call only
 *       when the supplied {@link HttpRequest} does not already declare {@code .timeout(...)}.
 *       Caller-set timeouts always win.</li>
 * </ul>
 *
 * <p>To customize, pass an {@link HttpClient} and/or an explicit fallback request timeout via the
 * full constructor. Pass {@code null} as the fallback to keep the JDK's "no timeout" behavior.
 *
 * <pre>{@code
 * // Defaults: connectTimeout=5s, fallback requestTimeout=30s
 * var httpCall = new HttpCallStep("call.inventory-api");
 *
 * // Tighter per-call budget — every request without an explicit timeout times out after 2s
 * var httpCall = new HttpCallStep("call.inventory-api", Duration.ofSeconds(2));
 *
 * // Custom client — e.g. with redirect policy, custom SSL, or custom connect timeout
 * var client = HttpClient.newBuilder()
 *     .connectTimeout(Duration.ofSeconds(3))
 *     .followRedirects(HttpClient.Redirect.NORMAL)
 *     .build();
 * var httpCall = new HttpCallStep("call.inventory-api", client, Duration.ofSeconds(2));
 *
 * // Wiring — caller builds the HttpRequest in a preceding inline step
 * Step<OrderRequest, HttpRequest> buildRequest = Step.builder(
 *         "build.request", OrderRequest.class, HttpRequest.class)
 *     .execute((order, ctx) -> HttpRequest.newBuilder()
 *         .uri(URI.create("https://api.example.com/inventory/" + order.sku()))
 *         .GET().build())
 *     .build();
 *
 * Pipeline<OrderRequest, String> pipeline = Pipeline.builder(OrderRequest.class)
 *     .then(buildRequest)
 *     .then(httpCall.withRetry(RetryPolicy.fixed(3, 0)))
 *     .build();
 * }</pre>
 */
public final class HttpCallStep implements Step<HttpRequest, String> {

    public static final long DEFAULT_CONNECT_TIMEOUT_SECONDS = 5L;
    public static final long DEFAULT_REQUEST_TIMEOUT_SECONDS = 30L;

    private static final HttpClient DEFAULT_CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(DEFAULT_CONNECT_TIMEOUT_SECONDS))
            .build();
    private static final Duration DEFAULT_REQUEST_TIMEOUT =
            Duration.ofSeconds(DEFAULT_REQUEST_TIMEOUT_SECONDS);

    private final HttpClient client;
    private final Duration fallbackRequestTimeout;
    private final StepDescriptor<HttpRequest, String> descriptor;

    public HttpCallStep(String id) {
        this(id, DEFAULT_CLIENT, DEFAULT_REQUEST_TIMEOUT);
    }

    public HttpCallStep(String id, HttpClient client) {
        this(id, client, DEFAULT_REQUEST_TIMEOUT);
    }

    public HttpCallStep(String id, Duration fallbackRequestTimeout) {
        this(id, DEFAULT_CLIENT, fallbackRequestTimeout);
    }

    /**
     * @param id                      step id, unique within the pipeline
     * @param client                  the {@link HttpClient} used for all calls; share across steps
     * @param fallbackRequestTimeout  per-call timeout applied only when the incoming
     *                                {@link HttpRequest} does not declare one; pass {@code null}
     *                                to keep the JDK default ("no timeout") for requests that
     *                                omit their own
     */
    public HttpCallStep(String id, HttpClient client, Duration fallbackRequestTimeout) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(client, "client");
        if (fallbackRequestTimeout != null
                && (fallbackRequestTimeout.isZero() || fallbackRequestTimeout.isNegative())) {
            throw new IllegalArgumentException(
                "fallbackRequestTimeout must be positive, got: " + fallbackRequestTimeout);
        }
        this.client = client;
        this.fallbackRequestTimeout = fallbackRequestTimeout;
        this.descriptor = StepDescriptor.builder(id, HttpRequest.class, String.class).build();
    }

    @Override
    public StepDescriptor<HttpRequest, String> describe() {
        return descriptor;
    }

    @Override
    public String execute(HttpRequest request, StepContext ctx) throws Exception {
        HttpRequest effective = request.timeout().isPresent() || fallbackRequestTimeout == null
                ? request
                : copyWithTimeout(request, fallbackRequestTimeout);
        HttpResponse<String> response = client.send(effective, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new HttpCallException(response.statusCode(), response.body());
        }
        return response.body();
    }

    private static HttpRequest copyWithTimeout(HttpRequest original, Duration timeout) {
        HttpRequest.Builder b = HttpRequest.newBuilder()
                .uri(original.uri())
                .method(original.method(),
                    original.bodyPublisher().orElse(HttpRequest.BodyPublishers.noBody()))
                .timeout(timeout);
        original.version().ifPresent(b::version);
        if (original.expectContinue()) {
            b.expectContinue(true);
        }
        original.headers().map().forEach((name, values) -> {
            for (String v : values) {
                b.header(name, v);
            }
        });
        return b.build();
    }
}
