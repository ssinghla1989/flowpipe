package io.flowpipe.commons.security;

import io.flowpipe.api.Step;
import io.flowpipe.api.StepContext;
import io.flowpipe.api.StepDescriptor;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.Jwts;

import java.security.PublicKey;
import javax.crypto.SecretKey;
import java.util.Objects;

/**
 * Validates a JWT token and extracts its claims using the JJWT library.
 *
 * <p>Validates signature, expiry ({@code exp}), and not-before ({@code nbf}) automatically.
 * Any violation throws a JJWT {@link io.jsonwebtoken.JwtException} subclass
 * (e.g. {@link io.jsonwebtoken.ExpiredJwtException}, {@link io.jsonwebtoken.security.SignatureException})
 * which FlowPipe surfaces as a {@code Failure}. A {@code "Bearer "} prefix is stripped automatically.
 *
 * <p>Construct via the factory methods — one for HMAC shared-secret algorithms (HS256/384/512),
 * one for asymmetric algorithms (RS256/384/512, ES256/384/512):
 *
 * <pre>{@code
 * // HMAC — shared secret (e.g. internal service-to-service)
 * SecretKey key = Keys.hmacShaKeyFor(Decoders.BASE64.decode(base64Secret));
 * var validateJwt = JwtValidationStep.hmac("auth.validate-jwt", key);
 *
 * // RSA / EC — public key from your identity provider
 * PublicKey publicKey = loadPublicKey();
 * var validateJwt = JwtValidationStep.asymmetric("auth.validate-jwt", publicKey);
 *
 * // Reading claims downstream via state
 * var pipeline = Pipeline.builder(String.class, OrderResponse.class)
 *     .then(validateJwt.withOutputKey(SecurityStateKeys.JWT_CLAIMS))
 *     .then(orderStep)
 *     .build();
 * }</pre>
 */
public final class JwtValidationStep implements Step<String, Claims> {

    private final JwtParser parser;
    private final StepDescriptor<String, Claims> descriptor;

    /**
     * Validates tokens signed with an HMAC algorithm (HS256, HS384, HS512).
     */
    public static JwtValidationStep hmac(String id, SecretKey signingKey) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(signingKey, "signingKey");
        JwtParser parser = Jwts.parser().verifyWith(signingKey).build();
        return new JwtValidationStep(id, parser);
    }

    /**
     * Validates tokens signed with an asymmetric algorithm (RS256/384/512, ES256/384/512).
     * Pass the identity provider's public key or certificate public key.
     */
    public static JwtValidationStep asymmetric(String id, PublicKey publicKey) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(publicKey, "publicKey");
        JwtParser parser = Jwts.parser().verifyWith(publicKey).build();
        return new JwtValidationStep(id, parser);
    }

    private JwtValidationStep(String id, JwtParser parser) {
        this.parser = parser;
        this.descriptor = StepDescriptor.builder(id, String.class, Claims.class).build();
    }

    @Override
    public StepDescriptor<String, Claims> describe() {
        return descriptor;
    }

    @Override
    public Claims execute(String token, StepContext ctx) {
        String stripped = token.startsWith("Bearer ") ? token.substring(7).strip() : token.strip();
        return parser.parseSignedClaims(stripped).getPayload();
    }
}
