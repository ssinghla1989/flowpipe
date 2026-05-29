package com.gpc.commons.http;

public final class HttpCallException extends RuntimeException {

    private final int statusCode;
    private final String responseBody;

    public HttpCallException(int statusCode, String responseBody) {
        super("HTTP call failed with status " + statusCode + ": " + truncate(responseBody));
        this.statusCode = statusCode;
        this.responseBody = responseBody;
    }

    public int statusCode() {
        return statusCode;
    }

    public String responseBody() {
        return responseBody;
    }

    private static String truncate(String body) {
        if (body == null) return "";
        return body.length() > 200 ? body.substring(0, 200) + "..." : body;
    }
}
