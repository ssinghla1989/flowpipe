### Requirement: Order Processing Pipeline Builder

The system SHALL provide a complete, runnable example of an order-processing pipeline that demonstrates FlowPipe's orchestration, composition, and observability capabilities in a realistic multi-step workflow.

#### Scenario: Pipeline accepts valid order input
- **WHEN** a valid order request is submitted with valid customer, items, and payment details
- **THEN** the pipeline processes the order through validation, inventory check, payment processing, notification, and logging steps

#### Scenario: Pipeline rejects invalid order
- **WHEN** an order with missing or invalid fields is submitted
- **THEN** the validation step fails and the pipeline returns a failure result without proceeding to downstream steps

#### Scenario: Inventory check runs in parallel with payment processing
- **WHEN** an order passes validation
- **THEN** inventory check and payment processing steps execute concurrently, not sequentially

#### Scenario: Notification step branches on inventory status
- **WHEN** inventory check completes with different stock statuses (in-stock vs. pre-order)
- **THEN** the notification step sends different confirmation messages based on the branch outcome

### Requirement: Payment Processing with Automatic Retries

The system SHALL implement a payment processing step that demonstrates automatic retry behavior with exponential backoff.

#### Scenario: Payment succeeds on first attempt
- **WHEN** a valid payment request is processed and the payment service responds with success
- **THEN** the payment step completes and returns the transaction result

#### Scenario: Payment retries on transient failure
- **WHEN** a payment request fails with a transient error (e.g., temporary service unavailable)
- **THEN** the pipeline automatically retries the payment step according to the configured retry policy and succeeds on the second attempt

#### Scenario: Payment fails after exhausting retries
- **WHEN** a payment request fails with a transient error and the maximum retry attempts are exhausted
- **THEN** the pipeline returns a failure result and does not proceed to the notification step

### Requirement: Observability Integration

The system SHALL emit structured logs and metrics at each step boundary using FlowPipe's observability mechanisms.

#### Scenario: Step execution metrics are recorded
- **WHEN** a step executes
- **THEN** a step outcome metric is recorded via the MetricsRecorder with the step name, result status, and duration

#### Scenario: Pipeline lifecycle is logged
- **WHEN** a pipeline starts, completes, or fails
- **THEN** structured SLF4J log entries are emitted with pipeline identifiers and step outcomes

#### Scenario: Error details are captured
- **WHEN** a step fails
- **THEN** the error details (exception type, message) are logged and the failure is recorded in observability metrics
