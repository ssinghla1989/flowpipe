package io.flowpipe.example;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * Simulates persisting an order record by POSTing to the JSONPlaceholder API.
 * POST https://jsonplaceholder.typicode.com/posts
 */
public final class HttpPersistenceService implements PersistenceService {

    private static final String API_URL = "https://jsonplaceholder.typicode.com/posts";
    private final HttpClient http = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(10))
        .build();

    @Override
    public void log(Order order, OrderConfirmation confirmation) throws Exception {
        String body = "{"
            + "\"title\":\"order-log\","
            + "\"body\":\"orderId=" + order.orderId()
            + " txn=" + confirmation.transactionId()
            + " status=" + confirmation.stockStatus() + "\","
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
            throw new RuntimeException("Persistence API returned HTTP " + response.statusCode());
        }
    }
}
