## Why

FlowPipe has been designed to orchestrate synchronous API pipelines with business logic, but lacks a concrete, realistic example that demonstrates how it handles complex, production-like scenarios. Testing against a real-world workflow—one that involves multiple API calls, business rule evaluation, conditional routing, retries, and failure handling—is essential to validate that the library meets its design goals and scales to the kinds of problems developers face.

## What Changes

- Add a comprehensive example pipeline that orchestrates a multi-step API workflow with realistic business rules
- The example includes: input validation, API composition, conditional branching based on business logic, error handling with retries, and observability instrumentation
- Create integration tests that exercise the full pipeline execution, including success paths, failure scenarios, and edge cases
- Serve as reference documentation for new users learning how to build pipelines in FlowPipe

## Capabilities

### New Capabilities
- `api-orchestration-example`: A complete, runnable example of orchestrating multiple APIs in sequence and parallel with business rule evaluation (e.g., order processing with inventory checks, payment processing, and notification)
- `pipeline-integration-tests`: Integration tests that validate realistic pipeline execution scenarios including validation failures, API errors, retries, conditional branching, and observability

### Modified Capabilities
<!-- Existing capabilities whose REQUIREMENTS are changing (not just implementation).
     Only list here if spec-level behavior changes. Each needs a delta spec file.
     Use existing spec names from openspec/specs/. Leave empty if no requirement changes. -->

## Impact

- New example/test code added to `flowpipe-test` or as a new integration test module
- Demonstrates real-world usage patterns for FlowPipe users
- Provides confidence that the library's design (sequential, parallel, conditional, retry mechanics) works correctly on realistic workflows
- No changes to the core FlowPipe API or existing behavior
