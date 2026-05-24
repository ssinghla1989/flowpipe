## Context

`README.md` was last substantially updated after parallel composition landed and still carries a one-line disclaimer saying branching, retry, and lifecycle hooks are not yet implemented. Five features have shipped since then: conditional branching, retry with backoff, lifecycle hooks, foreach fan-out, and per-step execution timeout. `CLAUDE.md` was last updated after lifecycle hooks and is missing foreach and timeout.

## Goals / Non-Goals

**Goals:**
- Bring README sections up to date with every shipped feature, using the same concise code-example style as the existing parallel and observability sections.
- Update CLAUDE.md Status bullet list and module/package layout to reflect the current state of `io.flowpipe.api` and `io.flowpipe.engine`.

**Non-Goals:**
- Full API reference or Javadoc generation.
- Changelog or migration guide (no breaking changes were made).
- Changes to any source code.

## Decisions

### One section per feature in README, ordered by composition complexity

Add sections in the logical composition order a new user would encounter them: branching → retry → lifecycle hooks → foreach → timeout. Each section gets: one sentence of purpose, a minimal code snippet using `Step.of(...)` or `StepDescriptor.builder(...)`, and a sentence or two of behavioural notes. This mirrors the existing parallel and observability sections.

### CLAUDE.md Status list stays as bullets, add two lines

The status list format is already established (one bullet per feature). Add two bullets at the end:
- Foreach fan-out: `foreach(step)` / `foreach(step, concurrency)` on `PipelineBuilder`
- Per-step timeout: `TimeoutPolicy` on `StepDescriptor`

### CLAUDE.md module layout: update `io.flowpipe.api` and `io.flowpipe.engine` entries

`io.flowpipe.api` needs `RetryPolicy`, `PipelineLifecycle`, `TimeoutPolicy`, `StepTimeoutException` added to the public surface list.
`io.flowpipe.engine` needs `BranchNode` and `ForeachNode` added to the internal sealed node types, and the `PipelineBuilder` method list updated to include `.branch(...)`, `.foreach(...)`, and `.withLifecycle(...)`.

### Hard-constraint bullet for timeout

Add: **Timeouts are configurable but invisible to step authors** — mirrors the existing retry constraint bullet.

## Risks / Trade-offs

- Code snippets in README can drift from the actual API if future refactors aren't reflected here. Mitigation: snippets are kept minimal (no import lists, no full class wrappers) to reduce the surface area that can go stale.
