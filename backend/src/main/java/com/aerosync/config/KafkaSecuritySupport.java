package com.aerosync.config;

import org.springframework.util.StringUtils;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

/**
 * Applies cloud Kafka security settings (Aiven free tier etc.) to a client property map.
 *
 * <p>Certificates come from base64-encoded PEM environment variables and are passed to Kafka's
 * native PEM support, so no keystore or certificate file ever has to be committed or mounted.
 * The client key must be an unencrypted PKCS#8 PEM ({@code -----BEGIN PRIVATE KEY-----}).
 */
public final class KafkaSecuritySupport {

    public record Settings(String securityProtocol,
                           String saslMechanism,
                           String saslJaasConfig,
                           String caCertB64,
                           String clientCertB64,
                           String clientKeyB64) {}

    private KafkaSecuritySupport() {}

    public static void apply(Map<String, Object> props, Settings s) {
        String protocol = StringUtils.hasText(s.securityProtocol()) ? s.securityProtocol().trim() : "PLAINTEXT";
        boolean hasCa = StringUtils.hasText(s.caCertB64());
        boolean hasClientCert = StringUtils.hasText(s.clientCertB64()) && StringUtils.hasText(s.clientKeyB64());
        if ("PLAINTEXT".equalsIgnoreCase(protocol) && (hasCa || hasClientCert)) {
            protocol = "SSL"; // certificates supplied without a protocol: TLS is clearly intended
        }
        props.put("security.protocol", protocol);

        if (StringUtils.hasText(s.saslMechanism())) {
            props.put("sasl.mechanism", s.saslMechanism().trim());
        }
        if (StringUtils.hasText(s.saslJaasConfig())) {
            props.put("sasl.jaas.config", s.saslJaasConfig().trim());
        }
        if (hasCa) {
            props.put("ssl.truststore.type", "PEM");
            props.put("ssl.truststore.certificates", decode(s.caCertB64(), "KAFKA_CA_CERT_B64"));
        }
        if (hasClientCert) {
            props.put("ssl.keystore.type", "PEM");
            props.put("ssl.keystore.certificate.chain", decode(s.clientCertB64(), "KAFKA_CLIENT_CERT_B64"));
            props.put("ssl.keystore.key", decode(s.clientKeyB64(), "KAFKA_CLIENT_KEY_B64"));
        }
    }

    static String decode(String base64, String name) {
        try {
            String pem = new String(Base64.getMimeDecoder().decode(base64.trim()), StandardCharsets.UTF_8).trim();
            if (!pem.startsWith("-----BEGIN")) {
                throw new IllegalStateException(name + " does not decode to a PEM document.");
            }
            return pem;
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException(name + " is not valid base64.", e);
        }
    }
}
