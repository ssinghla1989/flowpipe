dependencies {
    api(project(":flowpipe-core"))
    api("com.networknt:json-schema-validator:1.4.3")

    // JWT — api scope because Claims appears in JwtValidationStep's output type
    api("io.jsonwebtoken:jjwt-api:0.12.6")
    runtimeOnly("io.jsonwebtoken:jjwt-impl:0.12.6")
    runtimeOnly("io.jsonwebtoken:jjwt-jackson:0.12.6")

    testImplementation(project(":flowpipe-test"))
    testRuntimeOnly("ch.qos.logback:logback-classic:1.5.6")
}
