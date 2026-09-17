package com.aerosync.messaging;

import com.aerosync.model.FlightTelemetry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}"
})
@EmbeddedKafka(partitions = 1, topics = { TelemetryProducer.TOPIC })
public class KafkaIntegrationTest {

    @Autowired
    private TelemetryProducer producer;

    @MockitoSpyBean
    private TelemetryConsumer consumer;

    @Test
    void telemetrySuccessfullyTravelsThroughKafka() throws InterruptedException {
        FlightTelemetry telemetry = new FlightTelemetry(
                "AIC864", "123456", 12.0, 77.0, 10000.0, 250.0, System.currentTimeMillis()
        );

        // Give the consumer exactly 3 seconds to spin up and connect to the embedded topic
        Thread.sleep(3000);

        producer.publish(telemetry).block();

        // Verify the consumer caught the JSON string payload
        verify(consumer, timeout(10000).atLeastOnce()).consume(anyString());
    }
}