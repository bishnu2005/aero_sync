package com.aerosync.config;

import com.aerosync.messaging.TelemetryProducer;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka clients built from environment variables so the same image talks to local PLAINTEXT Kafka
 * or to a TLS/SASL cloud cluster (see {@link KafkaSecuritySupport}).
 */
@EnableKafka
@Configuration
public class KafkaConfig {

    private final String bootstrapServers;
    private final String consumerGroup;
    private final String autoOffsetReset;
    private final int topicPartitions;
    private final short topicReplicationFactor;
    private final KafkaSecuritySupport.Settings security;

    public KafkaConfig(
            @Value("${spring.kafka.bootstrap-servers:localhost:9092}") String bootstrapServers,
            @Value("${aerosync.kafka.consumer-group:aerosync-group}") String consumerGroup,
            @Value("${aerosync.kafka.auto-offset-reset:earliest}") String autoOffsetReset,
            @Value("${aerosync.kafka.topic-partitions:1}") int topicPartitions,
            @Value("${aerosync.kafka.topic-replication-factor:1}") short topicReplicationFactor,
            @Value("${spring.kafka.properties.security.protocol:PLAINTEXT}") String securityProtocol,
            @Value("${spring.kafka.properties.sasl.mechanism:}") String saslMechanism,
            @Value("${spring.kafka.properties.sasl.jaas.config:}") String saslJaasConfig,
            @Value("${aerosync.kafka.ssl.ca-cert-b64:}") String caCertB64,
            @Value("${aerosync.kafka.ssl.client-cert-b64:}") String clientCertB64,
            @Value("${aerosync.kafka.ssl.client-key-b64:}") String clientKeyB64) {
        this.bootstrapServers = bootstrapServers;
        this.consumerGroup = consumerGroup;
        this.autoOffsetReset = autoOffsetReset;
        this.topicPartitions = Math.max(1, topicPartitions);
        this.topicReplicationFactor = (short) Math.max(1, topicReplicationFactor);
        this.security = new KafkaSecuritySupport.Settings(securityProtocol, saslMechanism, saslJaasConfig,
                caCertB64, clientCertB64, clientKeyB64);
    }

    Map<String, Object> commonProperties() {
        Map<String, Object> props = new HashMap<>();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        KafkaSecuritySupport.apply(props, security);
        return props;
    }

    @Bean
    public KafkaAdmin kafkaAdmin() {
        KafkaAdmin admin = new KafkaAdmin(commonProperties());
        admin.setFatalIfBrokerNotAvailable(false);
        return admin;
    }

    @Bean
    public NewTopic telemetryTopic() {
        return TopicBuilder.name(TelemetryProducer.TOPIC)
                .partitions(topicPartitions)
                .replicas(topicReplicationFactor)
                .build();
    }

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> props = commonProperties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 20);
        // Fail fast instead of blocking a scheduler thread for 60 s when the broker is unreachable.
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10_000);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30_000);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = commonProperties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        // The listener never throws, but if it ever did: log and skip the record, never retry-loop.
        factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(0L, 0L)));
        return factory;
    }
}
