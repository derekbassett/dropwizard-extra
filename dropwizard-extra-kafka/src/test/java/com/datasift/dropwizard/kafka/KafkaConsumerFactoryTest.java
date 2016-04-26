package com.datasift.dropwizard.kafka;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.google.common.collect.ImmutableSet;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import io.dropwizard.setup.Environment;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.InetSocketAddress;
import java.util.Properties;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests {@link KafkaConsumerFactory}.
 */
public class KafkaConsumerFactoryTest {

    private TestKafkaConsumerFactory factory = null;

    private final Environment environment = Mockito.mock(Environment.class);
    private final LifecycleEnvironment lifecycleEnvironment = Mockito.mock(LifecycleEnvironment.class);

    private final MetricRegistry metricRegistry = Mockito.mock(MetricRegistry.class);
    private final Meter meter = Mockito.mock(Meter.class);
    private final HealthCheckRegistry healthCheckRegistry = Mockito.mock(HealthCheckRegistry.class);

    private String name;
    private String metricName;


    class TestKafkaConsumerFactory<K, V> extends KafkaConsumerFactory {
        public Properties properties;
        public Deserializer<K> keyDeserializer;
        public Deserializer<V> valueDeserializer;
        public Consumer<K, V> consumer = Mockito.mock(Consumer.class);

        public TestKafkaConsumerFactory() {
            super();
        }

        @Override
        protected <K, V> Consumer buildUnmanaged(final Properties properties,
                                                 final Deserializer<K> keyDeserializer,
                                                 final Deserializer<V> valueDeserializer) {
            return consumer;
        }
    }

    @Before
    public void setup() throws Exception {
        Mockito.reset(environment, lifecycleEnvironment, metricRegistry, meter, healthCheckRegistry);

        name = "name";
        metricName = MetricRegistry.name(name, "received");

        when(environment.metrics()).thenReturn(metricRegistry);
        when(environment.lifecycle()).thenReturn(lifecycleEnvironment);
        when(environment.healthChecks()).thenReturn(healthCheckRegistry);

        when(metricRegistry.meter(metricName)).thenReturn(meter);

        InetSocketAddress address = new InetSocketAddress("localhost",9092);
        factory = new TestKafkaConsumerFactory();
        factory.setBrokers(ImmutableSet.of(address));
    }

    @Test
    public void test_build_value_and_key_deserializer() throws Exception {
        String name = "name";

        Assertions.assertThat(
                factory.build(environment, ByteArrayDeserializer.class, ByteArrayDeserializer.class, name))
                .isNotNull();

        verify(metricRegistry).meter(metricName);
    }

    @Test
    public void test_build_value_deserializer() throws Exception {
        String name = "name";

        Assertions.assertThat(
                factory.build(environment, ByteArrayDeserializer.class, name))
                .isNotNull();

        verify(metricRegistry).meter(metricName);
    }

}
