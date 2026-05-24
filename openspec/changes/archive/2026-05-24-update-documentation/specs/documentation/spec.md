## ADDED Requirements

### Requirement: README accurately describes all shipped features

`README.md` SHALL contain a dedicated section for each shipped composition feature: sequential steps, parallel composition, conditional branching, retry with backoff, lifecycle hooks, foreach fan-out, and per-step execution timeout. Each section SHALL include a minimal code snippet illustrating the feature's primary API entry point (e.g., `PipelineBuilder` method or `StepDescriptor` wiring call). The file SHALL NOT contain any statement that a shipped feature is "not yet implemented."

#### Scenario: README contains a branching section

- **WHEN** a reader opens `README.md`
- **THEN** it MUST contain a section covering conditional branching via `PipelineBuilder.branch(...)` with a code example

#### Scenario: README contains a retry section

- **WHEN** a reader opens `README.md`
- **THEN** it MUST contain a section covering retry with backoff via `RetryPolicy` and `StepDescriptor.withRetry(...)` with a code example

#### Scenario: README contains a lifecycle hooks section

- **WHEN** a reader opens `README.md`
- **THEN** it MUST contain a section covering lifecycle hooks via `PipelineLifecycle` and `PipelineBuilder.withLifecycle(...)` with a code example

#### Scenario: README contains a foreach section

- **WHEN** a reader opens `README.md`
- **THEN** it MUST contain a section covering foreach fan-out via `PipelineBuilder.foreach(...)` with a code example

#### Scenario: README contains a per-step timeout section

- **WHEN** a reader opens `README.md`
- **THEN** it MUST contain a section covering per-step timeout via `TimeoutPolicy` and `StepDescriptor.withTimeout(...)` with a code example

#### Scenario: README contains no stale "not yet implemented" claim

- **WHEN** a reader opens `README.md`
- **THEN** it MUST NOT contain the string "not yet implemented" for any feature that has already shipped

### Requirement: CLAUDE.md Status list reflects all shipped features

`CLAUDE.md` SHALL list foreach fan-out and per-step execution timeout in the **Status** feature bullet list, with the same level of detail as existing bullets (builder method names, relevant types).

#### Scenario: CLAUDE.md lists foreach fan-out

- **WHEN** a Claude Code session loads `CLAUDE.md`
- **THEN** the Status section MUST mention `foreach` with the `PipelineBuilder` entry point and concurrency option

#### Scenario: CLAUDE.md lists per-step timeout

- **WHEN** a Claude Code session loads `CLAUDE.md`
- **THEN** the Status section MUST mention `TimeoutPolicy` on `StepDescriptor`

### Requirement: CLAUDE.md module layout reflects current public surface

The **Module and package layout** section of `CLAUDE.md` SHALL accurately list the current public types in `io.flowpipe.api` (including `RetryPolicy`, `PipelineLifecycle`, `TimeoutPolicy`, `StepTimeoutException`) and the current internal engine node types in `io.flowpipe.engine` (including `BranchNode` and `ForeachNode`). The `PipelineBuilder` method list SHALL include `.branch(...)`, `.foreach(...)`, and `.withLifecycle(...)`.

#### Scenario: CLAUDE.md api package lists TimeoutPolicy and StepTimeoutException

- **WHEN** a Claude Code session loads `CLAUDE.md`
- **THEN** the `io.flowpipe.api` package description MUST mention `TimeoutPolicy` and `StepTimeoutException`

#### Scenario: CLAUDE.md engine section lists BranchNode and ForeachNode

- **WHEN** a Claude Code session loads `CLAUDE.md`
- **THEN** the `io.flowpipe.engine` section MUST mention `BranchNode` and `ForeachNode` as internal sealed node types
