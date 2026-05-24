package io.flowpipe.example;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * Simulates payment processing by POSTing to the JSONPlaceholder API.
 * POST https://jsonplaceholder.typicode.com/posts
 * Returns the response {@code id} as the transaction ID.
 */
public final class HttpPaymentProcessor implements PaymentProcessor {

    private static final String API_URL = "https://jsonplaceholder.typicode.com/posts";
    private final HttpClient http = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(10))
        .build();

    @Override
    public PaymentResult process(Order order) throws Exception {
        String total = order.items().stream()
            .map(i -> i.unitPrice().multiply(java.math.BigDecimal.valueOf(i.quantity())))
            .reduce(java.math.BigDecimal.ZERO, java.math.BigDecimal::add)
            .toPlainString();

        String body = "{"
            + "\"title\":\"payment\","
            + "\"body\":\"orderId=" + order.orderId()
            + " customerId=" + order.customerId()
            + " amount=" + total + "\","
            + "\"userId\":1"
            + "}";

        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(API_URL))
            .timeout(Duration.ofSeconds(10))
            .header("Content-Type", "application/json; charset=UTF-8")
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build();

        HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 201) {
            throw new RuntimeException("Payment API returned HTTP " + response.statusCode());
        }

        int id = HttpInventoryService.extractInt(response.body(), "id");
        return new PaymentResult(
            "TXN-" + id,
            new java.math.BigDecimal(total)
        );
    }
}
