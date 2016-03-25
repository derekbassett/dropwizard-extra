package com.datasift.dropwizard.kafka;

import com.google.common.io.Resources;
import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.validation.BaseValidator;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Properties;

import static com.datasift.dropwizard.kafka.KafkaProducerFactory.DEFAULT_BROKER_PORT;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Tests {@link KafkaProducerFactory}
 */
public class KafkaProducerFactoryTest {

    private KafkaProducerFactory factory;
    private Properties config;

    @Before
    public void setup() throws Exception {
        factory = new ConfigurationFactory<>(KafkaProducerFactory.class, BaseValidator.newValidator(), Jackson.newObjectMapper(), "dw")
                .build(new File(Resources.getResource("yaml/producer.yaml").toURI()));
        config = KafkaProducerFactory.toProperties(factory, StringSerializer.class, null, null, "test");
    }

    @Test
    public void testExplicitBrokers() {
        assertThat("explicitly defined brokers are correctly parsed",
                config.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG),
                equalTo("localhost:4321,192.168.10.12:123,localhost:"
                        + DEFAULT_BROKER_PORT + ",192.168.4.21:" + DEFAULT_BROKER_PORT));
    }
}
