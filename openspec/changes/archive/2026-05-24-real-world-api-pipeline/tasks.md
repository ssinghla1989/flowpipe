## 1. Package Structure and Data Models

- [x] 1.1 Create `io.flowpipe.example` package in `flowpipe-test/src/main/java`
- [x] 1.2 Define `Order` and `OrderItem` data classes with fields: orderId, customerId, items (list), total price, delivery address
- [x] 1.3 Define `OrderValidation` result class with validation status and error messages
- [x] 1.4 Define `InventoryStatus` enum (IN_STOCK, PRE_ORDER, OUT_OF_STOCK) and `InventoryCheckResult` class
- [x] 1.5 Define `PaymentResult` class with transaction ID and status
- [x] 1.6 Define `OrderConfirmation` class with confirmation message, order status, and estimated delivery date (if pre-order)

## 2. External Service Mocks

- [x] 2.1 Create `InventoryServiceMock` with configurable behavior to return IN_STOCK, PRE_ORDER, or OUT_OF_STOCK for test scenarios
- [x] 2.2 Create `PaymentProcessorMock` with configurable behavior to simulate success, transient failures (for retry testing), and permanent failures
- [x] 2.3 Create `NotificationServiceMock` to capture sent confirmations without actually sending emails
- [x] 2.4 Create `PersistenceServiceMock` to capture logged orders in memory for test verification

## 3. Step Implementations

- [x] 3.1 Implement `ValidateOrderStep` that validates order input (non-null, valid quantities, existing items)
- [x] 3.2 Implement `CheckInventoryStep` that queries the inventory service and returns InventoryCheckResult
- [x] 3.3 Implement `ProcessPaymentStep` with @RetryPolicy annotation (max 2 attempts, 100ms initial delay, 2x multiplier) that processes payment and returns PaymentResult
- [x] 3.4 Implement `SendConfirmationStep` that receives InventoryCheckResult and PaymentResult and sends appropriate confirmation via NotificationServiceMock
- [x] 3.5 Implement `LogOrderStep` that records the completed order to persistence
- [x] 3.6 Organize all step implementations in `OrderProcessingSteps` class with static factory methods

## 4. Pipeline Builder and Orchestration

- [x] 4.1 Create `OrderProcessingPipeline` class with static builder method
- [x] 4.2 Wire sequential composition: ValidateOrder → (InventoryCheck & ProcessPayment in parallel) → SendConfirmation → LogOrder
- [x] 4.3 Implement conditional branching in SendConfirmationStep to handle IN_STOCK vs. PRE_ORDER notifications
- [x] 4.4 Configure a `MetricsRecorder` on the pipeline builder to capture step outcomes
- [x] 4.5 Wire lifecycle hooks (PipelineLifecycle) to log pipeline start, finish, and error events
- [x] 4.6 Expose a public `execute(Order)` method that returns `Result<OrderConfirmation>`

## 5. Integration Tests - Happy Path

- [x] 5.1 Create `OrderProcessingIntegrationTest` class with JUnit 5 test fixtures
- [x] 5.2 Add test: happy path with in-stock items, successful payment, confirmation email sent
- [x] 5.3 Add test: happy path with pre-order items, successful payment, pre-order confirmation email sent
- [x] 5.4 Assert on pipeline Result (success vs. failure, output data)
- [x] 5.5 Assert on recorded metrics using `RecordingMetricsRecorder` that all steps executed with correct outcomes

## 6. Integration Tests - Failure Scenarios

- [x] 6.1 Add test: validation failure with invalid order data → pipeline fails early
- [x] 6.2 Add test: inventory out-of-stock → pipeline succeeds but sends out-of-stock notification
- [x] 6.3 Add test: payment fails permanently → pipeline fails and no confirmation email sent
- [x] 6.4 Assert that early failure prevents execution of downstream steps (observability data confirms this)

## 7. Integration Tests - Retry Behavior

- [x] 7.1 Add test: payment transient failure (fails on first call, succeeds on second) → pipeline completes successfully
- [x] 7.2 Configure mock payment processor to fail once with a retryable exception
- [x] 7.3 Assert that exactly 2 payment attempts were made (via mock call count or metrics)
- [x] 7.4 Assert that final pipeline result is success despite the transient failure

## 8. Integration Tests - Concurrency and Edge Cases

- [x] 8.1 Add test: parallel inventory check and payment processing don't interfere (verify both execute independently)
- [x] 8.2 Add test: multiple consecutive pipeline executions maintain state isolation
- [x] 8.3 Add test: order with zero items fails validation
- [x] 8.4 Assert via RecordingMetricsRecorder that concurrent steps both recorded metrics

## 9. Code Organization and Documentation

- [x] 9.1 Add package-level Javadoc to `io.flowpipe.example` explaining the purpose of the example
- [x] 9.2 Add class-level comments to `OrderProcessingPipeline` explaining the pipeline structure
- [x] 9.3 Add comments to each step implementation explaining which FlowPipe feature is demonstrated
- [x] 9.4 Create a README in the example directory with instructions on running the example and studying the code

## 10. Final Validation

- [x] 10.1 Run all integration tests and verify they pass
- [x] 10.2 Verify that the example compiles without warnings
- [x] 10.3 Ensure the example can be used standalone (no external service dependencies in code)
- [x] 10.4 Confirm all observability metrics and logs are correctly emitted during test execution
