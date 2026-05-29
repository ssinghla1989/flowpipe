package io.flowpipe.commons.http;

import com.sun.net.httpserver.HttpServer;
import io.flowpipe.api.Failure;
import io.flowpipe.api.Result;
import io.flowpipe.api.Success;
import io.flowpipe.engine.Pipeline;
import io.flowpipe.state.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class HttpCallStepTest {

    private HttpServer server;
    private int port;

    @BeforeEach
    void startServer() throws Exception {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        port = server.getAddress().getPort();

        server.createContext("/ok", exchange -> {
            byte[] body = "{\"status\":\"ok\"}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });

        server.createContext("/not-found", exchange -> {
            byte[] body = "{\"error\":\"not found\"}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(404, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });

        server.createContext("/server-error", exchange -> {
            byte[] body = "{\"error\":\"internal\"}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(500, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });

        server.start();
    }

    @AfterEach
    void stopServer() {
        server.stop(0);
    }

    // --- descriptor ---

    @Test
    void hasCorrectDescriptor() {
        var step = new HttpCallStep("call.api");
        assertThat(step.describe().id()).isEqualTo("call.api");
        assertThat(step.describe().inputType()).isEqualTo(HttpRequest.class);
        assertThat(step.describe().outputType()).isEqualTo(String.class);
    }

    // --- execution via pipeline ---

    @Test
    void returnsBodyOnSuccess() {
        var step = new HttpCallStep("call.api");
        var pipeline = Pipeline.builder(HttpRequest.class).then(step).build();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/ok"))
                .GET().build();

        Result<String> result = pipeline.execute(request, RequestContext.empty());

        assertThat(result).isInstanceOf(Success.class);
        assertThat(((Success<String>) result).value()).isEqualTo("{\"status\":\"ok\"}");
    }

    @Test
    void failsWithHttpCallExceptionOn404() {
        var step = new HttpCallStep("call.api");
        var pipeline = Pipeline.builder(HttpRequest.class).then(step).build();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/not-found"))
                .GET().build();

        Result<String> result = pipeline.execute(request, RequestContext.empty());

        assertThat(result).isInstanceOf(Failure.class);
        var ex = (HttpCallException) ((Failure<String>) result).cause();
        assertThat(ex.statusCode()).isEqualTo(404);
        assertThat(ex.responseBody()).contains("not found");
    }

    @Test
    void failsWithHttpCallExceptionOn500() {
        var step = new HttpCallStep("call.api");
        var pipeline = Pipeline.builder(HttpRequest.class).then(step).build();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/server-error"))
                .GET().build();

        Result<String> result = pipeline.execute(request, RequestContext.empty());

        assertThat(result).isInstanceOf(Failure.class);
        assertThat(((Failure<String>) result).cause()).isInstanceOf(HttpCallException.class);
        assertThat(((HttpCallException) ((Failure<String>) result).cause()).statusCode()).isEqualTo(500);
    }

    @Test
    void failsWhenHostUnreachable() {
        var step = new HttpCallStep("call.api");
        var pipeline = Pipeline.builder(HttpRequest.class).then(step).build();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:1/unreachable"))
                .GET().build();

        Result<String> result = pipeline.execute(request, RequestContext.empty());

        assertThat(result).isInstanceOf(Failure.class);
        assertThat(((Failure<String>) result).cause()).isNotInstanceOf(HttpCallException.class);
    }

    // --- policy overrides (inherited from Step) ---

    @Test
    void withRetryReturnsDifferentInstanceWithPolicyApplied() {
        var step = new HttpCallStep("call.api");
        var withRetry = step.withRetry(io.flowpipe.api.RetryPolicy.fixed(3, 0));

        assertThat(withRetry).isNotSameAs(step);
        assertThat(withRetry.describe().retryPolicy().maxAttempts()).isEqualTo(3);
        assertThat(step.describe().retryPolicy().maxAttempts()).isEqualTo(1);
    }
}
