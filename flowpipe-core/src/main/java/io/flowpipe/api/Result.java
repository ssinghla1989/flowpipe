package io.flowpipe.api;

public sealed interface Result<O> permits Success, Failure {
}
