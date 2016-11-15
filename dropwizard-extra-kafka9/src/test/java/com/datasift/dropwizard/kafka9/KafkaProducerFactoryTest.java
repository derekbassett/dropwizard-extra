package com.datasift.dropwizard.kafka9;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.datasift.dropwizard.kafka9.producer.InstrumentedProducer;
import com.datasift.dropwizard.kafka9.serializer.JacksonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import io.dropwizard.configuration.YamlConfigurationFactory;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import io.dropwizard.setup.Environment;
import io.dropwizard.validation.BaseValidator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.Properties;

import static com.datasift.dropwizard.kafka9.KafkaProducerFactory.DEFAULT_BROKER_PORT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.when;

/**
 * Tests {@link KafkaProducerFactory}
 */
public class KafkaProducerFactoryTest {

    private KafkaProducerFactory factory;
    private Properties config;

    private final Environment environment = Mockito.mock(Environment.class);
    private final LifecycleEnvironment lifecycle = Mockito.mock(LifecycleEnvironment.class);
    private final MetricRegistry metricRegistry = Mockito.mock(MetricRegistry.class);
    private final Meter meter = Mockito.mock(Meter.class);

    class TestKafkaProducerFactory extends KafkaProducerFactory {
        public Producer producer = Mockito.mock(KafkaProducer.class);
        public Properties properties;
        public Serializer keySerializer;
        public Serializer valueSerializer;

        protected <K, V> Producer<K , V> buildUnmanaged(final Properties properties,
                                                        final Serializer<K> keySerializer,
                                                        final Serializer<V> valueSerializer) {
            this.properties = properties;
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
            return producer;
        }
    }

    @Before
    public void setup() throws Exception {
        Mockito.reset(environment, lifecycle, metricRegistry, meter);

        factory = new YamlConfigurationFactory<>(KafkaProducerFactory.class, BaseValidator.newValidator(), Jackson.newObjectMapper(), "dw")
                .build(new File(Resources.getResource("yaml/producer.yaml").toURI()));
        config = KafkaProducerFactory.toProperties(factory, StringSerializer.class, null, null, "test");

        when(environment.metrics()).thenReturn(metricRegistry);
        when(environment.lifecycle()).thenReturn(lifecycle);

    }

    @Test
    public void testExplicitBrokers() {
        // "explicitly defined brokers are correctly parsed"
        assertThat(config.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG))
                .as("explicitly defined brokers are correctly parsed")
                .isEqualTo("localhost:4321,192.168.10.12:123,localhost:" + DEFAULT_BROKER_PORT + ",192.168.4.21:" + DEFAULT_BROKER_PORT);
    }

    @Test
    public void testBuildProducer() {
        TestKafkaProducerFactory factory = new TestKafkaProducerFactory();
        factory.setBrokers(ImmutableSet.of(new InetSocketAddress("localhost",9092)));
        String name = "name";
        String metricName = MetricRegistry.name(name, "sent");

        when(metricRegistry.meter(metricName)).thenReturn(meter);

        ObjectMapper objectMapper = Jackson.newObjectMapper();
        JacksonSerializer<String> valueSerializer = new JacksonSerializer(objectMapper);
        JacksonSerializer<String> keySerializer = new JacksonSerializer(objectMapper);

        Producer<String, String> producer = factory.build(keySerializer, valueSerializer, environment, name);

        assertThat(producer).isInstanceOf(InstrumentedProducer.class);
        assertThat(factory.keySerializer).as("the key serializer passed in").isSameAs(keySerializer);
        assertThat(factory.valueSerializer).as("the value serializer passed in").isSameAs(valueSerializer);

        Mockito.verify(lifecycle).manage(isA(Managed.class));
    }


}
