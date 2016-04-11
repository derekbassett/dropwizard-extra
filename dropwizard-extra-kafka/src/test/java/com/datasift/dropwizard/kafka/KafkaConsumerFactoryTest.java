package com.datasift.dropwizard.kafka;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.datasift.dropwizard.kafka.consumer.ConsumerRecordProcessor;
import com.datasift.dropwizard.kafka.consumer.PollingProcessor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import io.dropwizard.setup.Environment;
import io.dropwizard.util.Duration;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import javax.validation.Validation;
import javax.validation.Validator;
import java.io.File;
import java.net.InetSocketAddress;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.*;

/**
 * Tests {@link KafkaConsumerFactory}.
 */
public class KafkaConsumerFactoryTest {

    private KafkaConsumerFactory factory = null;
    private TestKafkaConsumerFactory testKafkaConsumerFactory = null;

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

        final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();
        factory = new ConfigurationFactory<>(KafkaConsumerFactory.class, validator, Jackson.newObjectMapper(), "dw")
                .build(new File(Resources.getResource("yaml/consumer.yaml").toURI()));
        name = "name";
        metricName = MetricRegistry.name(name, "received");

        when(environment.metrics()).thenReturn(metricRegistry);
        when(environment.lifecycle()).thenReturn(lifecycleEnvironment);
        when(environment.healthChecks()).thenReturn(healthCheckRegistry);

        when(metricRegistry.meter(metricName)).thenReturn(meter);

        InetSocketAddress address = new InetSocketAddress("localhost",9092);
        testKafkaConsumerFactory = new TestKafkaConsumerFactory();
        testKafkaConsumerFactory.setBrokers(ImmutableSet.of(address));
    }

    @Test
    public void test_build_value_and_key_deserializer() throws Exception {
        String name = "name";

        Assertions.assertThat(
                testKafkaConsumerFactory.build(ByteArrayDeserializer.class, ByteArrayDeserializer.class, environment, name))
                .isNotNull();

        verify(lifecycleEnvironment).manage(isA(Managed.class));
        verify(metricRegistry).meter(metricName);
    }

    @Test
    public void test_build_value_deserializer() throws Exception {
        String name = "name";

        Assertions.assertThat(
                testKafkaConsumerFactory.build(ByteArrayDeserializer.class, environment, name))
                .isNotNull();

        verify(lifecycleEnvironment).manage(isA(Managed.class));
        verify(metricRegistry).meter(metricName);
    }

    @Test
    public void test_build_synchronous_processor() throws Exception {
        String name = "name";
        ConsumerRecordProcessor consumerRecordProcessor = Mockito.mock(ConsumerRecordProcessor.class);

        testKafkaConsumerFactory.setTopics(ImmutableList.of("foo","bar"));

        ScheduledExecutorService executor = Mockito.mock(ScheduledExecutorService.class);

        PollingProcessor<byte[], byte[]> processor = testKafkaConsumerFactory.processWith(consumerRecordProcessor).build(environment, executor, name);

        ArgumentCaptor<Managed> captor = ArgumentCaptor.forClass(Managed.class);

        verify(lifecycleEnvironment, times(2)).manage(captor.capture());
        assertThat(captor.getAllValues().size(), is(2));
        Assertions.assertThat(captor.getAllValues()).contains(processor);
        verify(lifecycleEnvironment).addServerLifecycleListener(processor);
    }

    @Test
    public void testGroup() {
        assertThat("group is correctly configured", factory.getGroup(), is("test"));
    }

    @Test
    public void testTopics() {
        assertThat("has correct partition configuration",
                   factory.getTopics(),
                   allOf(hasItem("foo"), hasItem("bar")));
    }

    @Test
    public void testRetryResetDelay() {
        assertThat("retryResetDelay is overridden to 3 seconds",
                factory.getRetryResetDelay(),
                is(Duration.seconds(3)));
    }
}
