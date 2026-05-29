package io.flowpipe.example;

import io.flowpipe.api.Result;
import io.flowpipe.api.Step;
import io.flowpipe.api.StepContext;
import io.flowpipe.api.StepDescriptor;
import io.flowpipe.api.Success;
import io.flowpipe.engine.Pipeline;
import io.flowpipe.engine.PipelineBuilder;

import java.util.List;

/**
 * Demonstrates {@code foreach} for bulk API fan-out.
 *
 * <p>Pipeline: a batch request carrying a list of order IDs is resolved
 * to the corresponding {@link Order} objects by calling a downstream service
 * once per ID, with up to 4 concurrent in-flight calls.
 *
 * <p>FlowPipe features exercised:
 * <ul>
 *   <li>{@code foreach(step, concurrency)} — per-element fan-out with bounded concurrency</li>
 *   <li>Per-item structured logging and metrics (automatic, no step-author code required)</li>
 * </ul>
 */
public final class BulkEnrichmentExample {

    private BulkEnrichmentExample() {}

    /** Represents a batch request: a list of order IDs to resolve. */
    public record BatchRequest(List<String> orderIds) {}

    /**
     * Builds the enrichment pipeline.
     *
     * <p>Structure:
     * <pre>
     *   extract-ids          (BatchRequest → List&lt;String&gt;)
     *     → foreach(fetch-order)   (String → Order, applied per element)
     * </pre>
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static Pipeline<BatchRequest, List<Order>> build(OrderLookupService lookupService) {
        Step<BatchRequest, List<String>> extractIds =
            (Step<BatchRequest, List<String>>) (Step) Step.builder(
                "extract-ids", BatchRequest.class, List.class).execute((req, ctx) -> req.orderIds()).build();

        StepDescriptor<String, Order> fetchDesc = StepDescriptor.builder("fetch-order", String.class, Order.class).build();
        Step<String, Order> fetchOrder = new Step<>() {
            @Override public StepDescriptor<String, Order> describe() { return fetchDesc; }
            @Override public Order execute(String orderId, StepContext ctx) {
                return lookupService.findById(orderId);
            }
        };

        return (Pipeline<BatchRequest, List<Order>>) (Pipeline) PipelineBuilder
            .start(BatchRequest.class)
            .then(extractIds)
            .foreach(fetchOrder)
            .build();
    }

    /** Minimal lookup SPI used by the example pipeline. */
    public interface OrderLookupService {
        Order findById(String orderId);
    }

    /** Quick smoke-test using an in-memory implementation. */
    public static void main(String[] args) {
        OrderLookupService lookup = orderId -> new Order(
            orderId, "cust-" + orderId, List.of(), "123 Main St");

        Pipeline<BatchRequest, List<Order>> pipeline = build(lookup);

        BatchRequest request = new BatchRequest(List.of("ORD-1", "ORD-2", "ORD-3"));
        Result<List<Order>> result = pipeline.execute(request);

        if (result instanceof Success<List<Order>> s) {
            s.value().forEach(o -> System.out.println("resolved: " + o.orderId()));
        }
    }
}
