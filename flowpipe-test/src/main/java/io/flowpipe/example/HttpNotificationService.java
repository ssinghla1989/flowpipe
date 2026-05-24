package io.flowpipe.example;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * Simulates sending a confirmation email by POSTing to the JSONPlaceholder API.
 * POST https://jsonplaceholder.typicode.com/posts
 */
public final class HttpNotificationService implements NotificationService {

    private static final String API_URL = "https://jsonplaceholder.typicode.com/posts";
    private final HttpClient http = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(10))
        .build();

    @Override
    public void send(Order order, OrderConfirmation confirmation) throws Exception {
        // Escape double-quotes so the JSON body stays valid
        String safeMessage = confirmation.message().replace("\\", "\\\\").replace("\"", "\\\"");
        String body = "{"
            + "\"title\":\"Order Confirmation " + confirmation.orderId() + "\","
            + "\"body\":\"" + safeMessage + "\","
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
            throw new RuntimeException("Notification API returned HTTP " + response.statusCode());
        }
    }
}
