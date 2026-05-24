package io.flowpipe.example;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.LocalDate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Checks product stock by calling the DummyJSON products API.
 * GET https://dummyjson.com/products/{productId}
 */
public final class HttpInventoryService implements InventoryService {

    private static final String BASE_URL = "https://dummyjson.com";
    private final HttpClient http = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(10))
        .build();

    @Override
    public InventoryCheckResult check(Order order) throws Exception {
        String productId = order.items().get(0).productId();
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(BASE_URL + "/products/" + productId))
            .timeout(Duration.ofSeconds(10))
            .header("Accept", "application/json")
            .GET()
            .build();

        HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new RuntimeException(
                "Inventory API returned HTTP " + response.statusCode() + " for product " + productId);
        }

        int stock = extractInt(response.body(), "stock");
        if (stock > 10) {
            return new InventoryCheckResult(InventoryStatus.IN_STOCK, null);
        } else if (stock > 0) {
            return new InventoryCheckResult(InventoryStatus.PRE_ORDER, LocalDate.now().plusWeeks(2));
        } else {
            return new InventoryCheckResult(InventoryStatus.OUT_OF_STOCK, null);
        }
    }

    static int extractInt(String json, String field) {
        Matcher m = Pattern.compile("\"" + field + "\":\\s*(\\d+)").matcher(json);
        return m.find() ? Integer.parseInt(m.group(1)) : 0;
    }

    static String extractString(String json, String field) {
        Matcher m = Pattern.compile("\"" + field + "\":\\s*\"([^\"\\\\]*)\"").matcher(json);
        return m.find() ? m.group(1) : "";
    }
}
