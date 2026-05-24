package io.flowpipe.api;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public final class ExecutionTrace {

    private static final ExecutionTrace EMPTY = new ExecutionTrace(Collections.emptyList());

    private final List<TraceEntry> entries;

    private ExecutionTrace(List<TraceEntry> entries) {
        this.entries = entries;
    }

    public static ExecutionTrace empty() {
        return EMPTY;
    }

    public List<TraceEntry> entries() {
        return entries;
    }

    public int size() {
        return entries.size();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private final List<TraceEntry> entries = new ArrayList<>();

        private Builder() {
        }

        public Builder append(TraceEntry entry) {
            entries.add(Objects.requireNonNull(entry, "entry"));
            return this;
        }

        public ExecutionTrace build() {
            if (entries.isEmpty()) {
                return EMPTY;
            }
            return new ExecutionTrace(Collections.unmodifiableList(new ArrayList<>(entries)));
        }
    }
}
