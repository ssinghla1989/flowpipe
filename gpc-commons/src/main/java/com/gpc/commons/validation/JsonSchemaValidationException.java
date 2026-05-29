package com.gpc.commons.validation;

import com.networknt.schema.ValidationMessage;

import java.util.Set;
import java.util.stream.Collectors;

public final class JsonSchemaValidationException extends RuntimeException {

    private final String stepId;
    private final Set<ValidationMessage> violations;

    JsonSchemaValidationException(String stepId, Set<ValidationMessage> violations) {
        super("JSON schema validation failed at step '" + stepId + "': " + format(violations));
        this.stepId = stepId;
        this.violations = Set.copyOf(violations);
    }

    public String stepId() {
        return stepId;
    }

    public Set<ValidationMessage> violations() {
        return violations;
    }

    private static String format(Set<ValidationMessage> violations) {
        return violations.stream()
                .map(ValidationMessage::getMessage)
                .collect(Collectors.joining("; "));
    }
}
