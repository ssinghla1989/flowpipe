package io.flowpipe.commons.security;

import io.flowpipe.api.Failure;
import io.flowpipe.api.Result;
import io.flowpipe.api.Success;
import io.flowpipe.engine.Pipeline;
import io.flowpipe.state.RequestContext;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.ExpiredJwtException;
import io.jsonwebtoken.Jwts;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.crypto.SecretKey;
import java.security.KeyPair;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;

class JwtValidationStepTest {

    private static SecretKey hmacKey;
    private static KeyPair rsaKeyPair;

    @BeforeAll
    static void generateKeys() throws Exception {
        hmacKey = Jwts.SIG.HS256.key().build();
        rsaKeyPair = java.security.KeyPairGenerator.getInstance("RSA")
                .generateKeyPair();
    }

    private String mintToken(SecretKey key, String subject, Instant expiry) {
        var builder = Jwts.builder().subject(subject).issuedAt(new Date());
        if (expiry != null) builder.expiration(Date.from(expiry));
        return builder.signWith(key).compact();
    }

    private String mintToken(java.security.PrivateKey key, String subject, Instant expiry) {
        var builder = Jwts.builder().subject(subject).issuedAt(new Date());
        if (expiry != null) builder.expiration(Date.from(expiry));
        return builder.signWith(key).compact();
    }

    // --- descriptor ---

    @Test
    void hasCorrectDescriptor() {
        var step = JwtValidationStep.hmac("auth.validate-jwt", hmacKey);
        assertThat(step.describe().id()).isEqualTo("auth.validate-jwt");
        assertThat(step.describe().inputType()).isEqualTo(String.class);
        assertThat(step.describe().outputType()).isEqualTo(Claims.class);
    }

    // --- HMAC ---

    @Test
    void validHmacTokenReturnsClaimsWithCorrectSubject() {
        var step = JwtValidationStep.hmac("auth.validate-jwt", hmacKey);
        var pipeline = Pipeline.builder(String.class).then(step).build();

        String token = mintToken(hmacKey, "user-123", Instant.now().plus(1, ChronoUnit.HOURS));
        Result<Claims> result = pipeline.execute(token, RequestContext.empty());

        assertThat(result).isInstanceOf(Success.class);
        assertThat(((Success<Claims>) result).value().getSubject()).isEqualTo("user-123");
    }

    @Test
    void bearerPrefixIsStrippedAutomatically() {
        var step = JwtValidationStep.hmac("auth.validate-jwt", hmacKey);
        var pipeline = Pipeline.builder(String.class).then(step).build();

        String token = "Bearer " + mintToken(hmacKey, "user-123", Instant.now().plus(1, ChronoUnit.HOURS));
        Result<Claims> result = pipeline.execute(token, RequestContext.empty());

        assertThat(result).isInstanceOf(Success.class);
        assertThat(((Success<Claims>) result).value().getSubject()).isEqualTo("user-123");
    }

    @Test
    void expiredHmacTokenFails() {
        var step = JwtValidationStep.hmac("auth.validate-jwt", hmacKey);
        var pipeline = Pipeline.builder(String.class).then(step).build();

        String token = mintToken(hmacKey, "user-123", Instant.now().minus(1, ChronoUnit.HOURS));
        Result<Claims> result = pipeline.execute(token, RequestContext.empty());

        assertThat(result).isInstanceOf(Failure.class);
        assertThat(((Failure<Claims>) result).cause()).isInstanceOf(ExpiredJwtException.class);
    }

    @Test
    void tokenSignedWithWrongKeyFails() {
        var step = JwtValidationStep.hmac("auth.validate-jwt", hmacKey);
        var pipeline = Pipeline.builder(String.class).then(step).build();

        SecretKey wrongKey = Jwts.SIG.HS256.key().build();
        String token = mintToken(wrongKey, "user-123", Instant.now().plus(1, ChronoUnit.HOURS));
        Result<Claims> result = pipeline.execute(token, RequestContext.empty());

        assertThat(result).isInstanceOf(Failure.class);
        assertThat(((Failure<Claims>) result).cause())
                .isInstanceOf(io.jsonwebtoken.security.SignatureException.class);
    }

    @Test
    void malformedTokenFails() {
        var step = JwtValidationStep.hmac("auth.validate-jwt", hmacKey);
        var pipeline = Pipeline.builder(String.class).then(step).build();

        Result<Claims> result = pipeline.execute("not.a.jwt", RequestContext.empty());

        assertThat(result).isInstanceOf(Failure.class);
        assertThat(((Failure<Claims>) result).cause())
                .isInstanceOf(io.jsonwebtoken.JwtException.class);
    }

    // --- RSA ---

    @Test
    void validRsaTokenReturnsClaimsWithCorrectSubject() {
        var step = JwtValidationStep.asymmetric("auth.validate-jwt", rsaKeyPair.getPublic());
        var pipeline = Pipeline.builder(String.class).then(step).build();

        String token = mintToken(rsaKeyPair.getPrivate(), "svc-account", Instant.now().plus(1, ChronoUnit.HOURS));
        Result<Claims> result = pipeline.execute(token, RequestContext.empty());

        assertThat(result).isInstanceOf(Success.class);
        assertThat(((Success<Claims>) result).value().getSubject()).isEqualTo("svc-account");
    }

    // --- policy override (inherited from Step) ---

    @Test
    void withRetryReturnsDifferentInstanceWithPolicyApplied() {
        var step = JwtValidationStep.hmac("auth.validate-jwt", hmacKey);
        var withRetry = step.withRetry(io.flowpipe.api.RetryPolicy.fixed(2, 0));

        assertThat(withRetry).isNotSameAs(step);
        assertThat(withRetry.describe().retryPolicy().maxAttempts()).isEqualTo(2);
        assertThat(step.describe().retryPolicy().maxAttempts()).isEqualTo(1);
    }
}
