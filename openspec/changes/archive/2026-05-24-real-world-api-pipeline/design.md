## Context

FlowPipe is a Java library for composing synchronous API pipelines with sequential, parallel, conditional, and retry semantics. The library includes features like build-time wiring validation, structured observability (SLF4J logging and MetricsRecorder instrumentation), and lifecycle hooks. Currently, the test suite validates individual features in isolation, but lacks a realistic, end-to-end example that demonstrates how these features integrate in a production-like scenario.

This design describes a concrete order-processing pipeline that exercises the full breadth of FlowPipe's capabilities.

## Goals / Non-Goals

**Goals:**
- Demonstrate a realistic multi-step API pipeline with business logic and error handling
- Exercise all major FlowPipe features: sequential steps, parallel composition, conditional branching, retries, and observability hooks
- Provide runnable code that users can study as a reference implementation
- Build confidence that FlowPipe handles complex, real-world workflows correctly
- Serve as the foundation for integration tests covering success and failure scenarios

**Non-Goals:**
- Build a production order-processing system (this is an example, not a framework)
- Test external systems or real payment/inventory APIs (all external calls will be mocked)
- Document general API orchestration patterns beyond what FlowPipe demonstrates
- Create a UI or web service wrapper around the pipeline

## Decisions

### Scenario: Order Processing Pipeline

**Decision**: Use a multi-step e-commerce order processing workflow as the example.

**Rationale**: Order processing is a realistic, familiar use case that naturally requires sequential validation, parallel composition (inventory check + payment processing can happen in parallel), conditional branching (different handling for pre-order vs. in-stock items), and retries (payment transience). It exercises all of FlowPipe's features in a single, cohesive narrative.

**Pipeline structure**:
1. **Validate**: Check that the order is well-formed (items exist, quantities valid, customer has address)
2. **Check Inventory** (parallel with payment): Determine if items are in stock or pre-order
3. **Process Payment** (parallel with inventory): Charge the customer, with retry on transient failures
4. **Conditional Notification**: Send different confirmation emails based on stock status (in-stock → ship immediately, pre-order → estimated date)
5. **Log Order**: Record the completed order to persistence

Steps 2 and 3 run in parallel; step 4 branches on the inventory result. Steps 1 and 5 are purely sequential.

### Code Organization

**Decision**: Place the example pipeline and its supporting mocks in `flowpipe-test` under a new package `io.flowpipe.example`.

**Rationale**: The example and its tests are reference material for users, not part of the core library. Placing it in `flowpipe-test` (which is already a public artifact) makes it easy for users to import and study. It avoids polluting `flowpipe-core` with example code.

**Structure**:
- `OrderProcessingPipeline.java` — the main pipeline builder and orchestration logic
- `OrderProcessingSteps.java` — individual step implementations (validation, inventory check, payment, notification, logging)
- `ExternalServiceMocks.java` — mock implementations of inventory, payment, and notification services
- `OrderProcessingIntegrationTest.java` — test cases covering happy path, failures, retries, and edge cases

### Mocking External Services

**Decision**: Create simple, deterministic mock implementations of external services (inventory service, payment processor, email service) that can be configured per test to return specific outcomes.

**Rationale**: This keeps the example self-contained and runnable without external dependencies. Mocks can be configured to simulate both success and failure scenarios (e.g., payment service throws a retryable exception on first call, succeeds on second).

### Observability Integration

**Decision**: Wire the pipeline with a `MetricsRecorder` that records step outcomes. Use structured SLF4J logging to capture the pipeline lifecycle.

**Rationale**: Demonstrates how to integrate observability into a real pipeline, which is a key design goal of FlowPipe. Tests will use `RecordingMetricsRecorder` to assert that the expected steps executed and metrics were recorded.

### Testing Strategy

**Decision**: Create integration tests that cover:
1. Happy path: order validates → inventory in-stock → payment succeeds → notification sent → order logged
2. Retry scenario: payment fails transiently, then succeeds
3. Failure path: inventory out-of-stock, payment fails, notification not sent
4. Conditional branching: pre-order vs. in-stock handling differs

**Rationale**: Tests validate that FlowPipe's orchestration semantics work correctly on a realistic workflow. Each test configures mocks to simulate a specific scenario, executes the pipeline, and asserts on outcomes using both the pipeline's `Result` and the `RecordingMetricsRecorder`.

## Risks / Trade-offs

- **Risk**: Example becomes complex and obscures core FlowPipe concepts. **Mitigation**: Keep the pipeline simple (5-6 steps), document the purpose of each step, and provide inline comments highlighting which FlowPipe feature is being demonstrated.
- **Risk**: Mocks are too tightly coupled to implementation details, making tests fragile. **Mitigation**: Mocks implement simple, stable interfaces. Tests focus on observable behavior (pipeline result, metrics) rather than mock call counts.
- **Risk**: Examples become outdated as FlowPipe evolves. **Mitigation**: Examples should be reviewed as part of any API change to the core library. CLAUDE.md will note this as a constraint for future work.

## Open Questions

- None identified; the approach is straightforward and aligns with FlowPipe's existing design.
