package io.flowpipe.state;

import java.util.Objects;

public final class StateKey<T> {

    private final String name;
    private final Class<T> type;

    private StateKey(String name, Class<T> type) {
        this.name = Objects.requireNonNull(name, "name");
        this.type = Objects.requireNonNull(type, "type");
        if (name.isEmpty()) {
            throw new IllegalArgumentException("StateKey name must not be empty");
        }
    }

    public static <T> StateKey<T> of(String name, Class<T> type) {
        return new StateKey<>(name, type);
    }

    public String name() {
        return name;
    }

    public Class<T> type() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StateKey<?> other)) return false;
        return name.equals(other.name) && type.equals(other.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type);
    }

    @Override
    public String toString() {
        return "StateKey[" + name + ":" + type.getSimpleName() + "]";
    }
}
