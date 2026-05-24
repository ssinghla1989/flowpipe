# Order-Processing Pipeline Example

A complete, runnable demonstration of FlowPipe's orchestration capabilities using a realistic
e-commerce order-processing workflow.

## What This Example Shows

| FlowPipe Feature | Where to Look |
|---|---|
| Input validation | `OrderProcessingSteps.validateOrder()` |
| Sequential composition | `OrderProcessingPipeline.build()` — the `.then()` chain |
| Parallel composition | `parallel2(checkInventory, calculateOrderTotal)` |
| Shared mutable state | `ORDER_KEY` / `ORDER_CONTEXT_KEY` in `OrderProcessingSteps` |
| Retry policy (2× backoff) | `PAYMENT_RETRY` on `processPayment` step descriptor |
| Conditional logic in a step | `sendConfirmation` — switch on `InventoryStatus` |
| MetricsRecorder SPI | `RecordingMetricsRecorder` wired in `OrderProcessingPipeline.build()` |
| PipelineLifecycle hooks | `loggingLifecycle()` in `OrderProcessingPipeline` |

## Pipeline Structure

```
validate-order
  → [check-inventory ‖ calculate-order-total]   (parallel)
    → process-payment                            (sequential, 2-attempt retry)
      → send-confirmation                        (sequential, reads state)
        → log-order                              (sequential)
```

## Running the Tests

```bash
./gradlew :flowpipe-test:test --tests "io.flowpipe.example.*"
```

The 12 integration tests in `OrderProcessingIntegrationTest` cover:
- Happy paths (in-stock and pre-order)
- Validation failures (blank customer ID, empty item list)
- Out-of-stock notification path
- Permanent payment failure
- Transient payment failure with automatic retry
- Parallel step independence and metrics
- State isolation across multiple pipeline executions

## Key Files

| File | Purpose |
|---|---|
| `OrderProcessingPipeline.java` | Wires all steps into a `Pipeline<Order, OrderConfirmation>` |
| `OrderProcessingSteps.java` | Step factory methods, one per pipeline stage |
| `InventoryServiceMock.java` | Configurable inventory service for tests |
| `PaymentProcessorMock.java` | Configurable payment processor (succeed, fail-once, always-fail) |
| `NotificationServiceMock.java` | Captures sent confirmations for assertion |
| `PersistenceServiceMock.java` | Captures logged orders for assertion |

## How to Study This Code

1. **Start with `OrderProcessingPipeline.build()`** to see the full pipeline wiring
2. **Read `OrderProcessingSteps`** to understand each step and what FlowPipe feature it uses
3. **Read `OrderProcessingIntegrationTest`** to see how to test each scenario
4. **Experiment** by configuring mocks differently to observe pipeline behavior
