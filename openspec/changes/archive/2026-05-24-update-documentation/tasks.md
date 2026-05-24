## 1. README — remove stale notice and add missing sections

- [x] 1.1 Remove the sentence "Branching, retry, and lifecycle hooks are not yet implemented — they will land in subsequent changes." from `README.md`
- [x] 1.2 Add a **Conditional branching** section to `README.md` with a short description and a code snippet showing `PipelineBuilder.branch(branchId, predicate, ifTrue, ifFalse)`
- [x] 1.3 Add a **Retry with backoff** section to `README.md` with a short description and a code snippet showing `StepDescriptor.builder(...).withRetry(RetryPolicy.fixed(...)).build()`
- [x] 1.4 Add a **Lifecycle hooks** section to `README.md` with a short description and a code snippet showing `PipelineBuilder.withLifecycle(new PipelineLifecycle<>() { ... })`
- [x] 1.5 Add a **Foreach fan-out** section to `README.md` with a short description and a code snippet showing `PipelineBuilder.foreach(step)` / `foreach(step, concurrency)`
- [x] 1.6 Add a **Per-step timeout** section to `README.md` with a short description and a code snippet showing `StepDescriptor.builder(...).withTimeout(TimeoutPolicy.ofMillis(...)).build()`

## 2. CLAUDE.md — Status list

- [x] 2.1 Add a bullet for foreach fan-out to the **Status** feature list in `CLAUDE.md`, mentioning `.foreach(step)` / `.foreach(step, concurrency)` on `PipelineBuilder` and configurable concurrency with windowed parallel execution
- [x] 2.2 Add a bullet for per-step execution timeout to the **Status** feature list in `CLAUDE.md`, mentioning `TimeoutPolicy` on `StepDescriptor` with `withTimeout(...)`, `StepTimeoutException` as the failure cause, and per-attempt deadline semantics

## 3. CLAUDE.md — Module and package layout

- [x] 3.1 Update the `io.flowpipe.api` entry to add `RetryPolicy`, `PipelineLifecycle<I,O>`, `TimeoutPolicy`, and `StepTimeoutException` to the public surface list
- [x] 3.2 Update the `io.flowpipe.engine` entry to add `BranchNode` and `ForeachNode` to the internal sealed node types list, and add `.branch(...)`, `.foreach(...)`, and `.withLifecycle(...)` to the `PipelineBuilder` method list

## 4. CLAUDE.md — Hard constraints

- [x] 4.1 Add a hard-constraint bullet: **Timeouts are configurable but invisible to step authors** — the `execute` method sees one call per attempt, the framework enforces the deadline
