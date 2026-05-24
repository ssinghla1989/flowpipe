## Why

`README.md` still says "branching, retry, and lifecycle hooks are not yet implemented," and `CLAUDE.md` is missing the two most recently shipped features (foreach fan-out and per-step execution timeout). New contributors reading either file get a misleading picture of what the library can do.

## What Changes

- **README.md** — remove the stale "not yet implemented" notice; add sections for conditional branching, retry with backoff, lifecycle hooks, foreach fan-out, and per-step execution timeout with concise code examples matching the style of existing sections.
- **CLAUDE.md** — update the **Status** bullet list to include foreach fan-out and per-step execution timeout; update the **Module and package layout** section to reflect the current `io.flowpipe.api` and `io.flowpipe.engine` public surfaces (new types: `TimeoutPolicy`, `StepTimeoutException`, `RetryPolicy`, `PipelineLifecycle`; new engine nodes: `BranchNode`, `ForeachNode`); add a hard-constraint bullet for timeout invisibility to step authors.

## Capabilities

### New Capabilities

### Modified Capabilities

## Impact

- `README.md` — user-facing documentation only, no code changes.
- `CLAUDE.md` — Claude Code session guidance only, no code changes.
- No API, dependency, or build changes.
