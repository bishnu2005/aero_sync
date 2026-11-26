package com.aerosync.config;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaSecuritySupportTest {

    private static final String CA = "-----BEGIN CERTIFICATE-----\nMIIBca\n-----END CERTIFICATE-----";
    private static final String CERT = "-----BEGIN CERTIFICATE-----\nMIIBcert\n-----END CERTIFICATE-----";
    private static final String KEY = "-----BEGIN PRIVATE KEY-----\nMIIEkey\n-----END PRIVATE KEY-----";

    private static String b64(String s) {
        return Base64.getEncoder().encodeToString(s.getBytes(StandardCharsets.UTF_8));
    }

    private static Map<String, Object> apply(KafkaSecuritySupport.Settings settings) {
        Map<String, Object> props = new HashMap<>();
        KafkaSecuritySupport.apply(props, settings);
        return props;
    }

    @Test
    void localDefaultsArePlaintextWithNoSslProperties() {
        Map<String, Object> props = apply(new KafkaSecuritySupport.Settings("PLAINTEXT", "", "", "", "", ""));
        assertThat(props).containsOnly(Map.entry("security.protocol", "PLAINTEXT"));
    }

    @Test
    void pemCertificatesMapToNativeKafkaPemProperties() {
        Map<String, Object> props = apply(new KafkaSecuritySupport.Settings("SSL", null, null, b64(CA), b64(CERT), b64(KEY)));

        assertThat(props)
                .containsEntry("security.protocol", "SSL")
                .containsEntry("ssl.truststore.type", "PEM")
                .containsEntry("ssl.truststore.certificates", CA)
                .containsEntry("ssl.keystore.type", "PEM")
                .containsEntry("ssl.keystore.certificate.chain", CERT)
                .containsEntry("ssl.keystore.key", KEY);
    }

    @Test
    void certificatesWithoutProtocolUpgradeToSsl() {
        Map<String, Object> props = apply(new KafkaSecuritySupport.Settings("", null, null, b64(CA), null, null));
        assertThat(props).containsEntry("security.protocol", "SSL").doesNotContainKey("ssl.keystore.type");
    }

    @Test
    void saslSslSettingsArePassedThrough() {
        String jaas = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"u\" password=\"p\";";
        Map<String, Object> props = apply(new KafkaSecuritySupport.Settings("SASL_SSL", "SCRAM-SHA-256", jaas, b64(CA), "", ""));

        assertThat(props)
                .containsEntry("security.protocol", "SASL_SSL")
                .containsEntry("sasl.mechanism", "SCRAM-SHA-256")
                .containsEntry("sasl.jaas.config", jaas)
                .containsEntry("ssl.truststore.type", "PEM");
    }

    @Test
    void clientCertWithoutKeyIsIgnored() {
        Map<String, Object> props = apply(new KafkaSecuritySupport.Settings("SSL", null, null, null, b64(CERT), ""));
        assertThat(props).doesNotContainKey("ssl.keystore.type");
    }

    @Test
    void invalidBase64FailsFastWithVariableName() {
        assertThatThrownBy(() -> apply(new KafkaSecuritySupport.Settings("SSL", null, null, "***not base64***", null, null)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("KAFKA_CA_CERT_B64");
        assertThatThrownBy(() -> apply(new KafkaSecuritySupport.Settings("SSL", null, null, b64("hello"), null, null)))
                .hasMessageContaining("PEM");
    }

    @Test
    void kafkaConfigBuildsSecuredClientProperties() {
        KafkaConfig config = new KafkaConfig("broker:12345", "g", "earliest", 1, (short) 1,
                "SSL", "", "", b64(CA), b64(CERT), b64(KEY));
        Map<String, Object> props = config.commonProperties();
        assertThat(props).containsEntry("bootstrap.servers", "broker:12345").containsEntry("ssl.keystore.key", KEY);
        assertThat(config.telemetryTopic().name()).isEqualTo("telemetry.flights.live");
        assertThat(config.telemetryTopic().numPartitions()).isEqualTo(1);
        assertThat(config.telemetryTopic().replicationFactor()).isEqualTo((short) 1);
    }
}
