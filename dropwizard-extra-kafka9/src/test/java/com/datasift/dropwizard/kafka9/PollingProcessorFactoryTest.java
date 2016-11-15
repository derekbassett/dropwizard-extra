package com.datasift.dropwizard.kafka9;

import com.codahale.metrics.health.HealthCheckRegistry;
import com.datasift.dropwizard.kafka9.consumer.ConsumerRecordProcessor;
import com.datasift.dropwizard.kafka9.consumer.PollingProcessor;
import com.google.common.collect.ImmutableList;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import io.dropwizard.lifecycle.setup.ScheduledExecutorServiceBuilder;
import io.dropwizard.setup.Environment;
import org.apache.kafka.clients.consumer.Consumer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.internal.stubbing.defaultanswers.ReturnsEmptyValues;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests {@link PollingProcessorFactory}.
 */
public class PollingProcessorFactoryTest {

    private PollingProcessorFactory factory = null;

    private final Environment environment = Mockito.mock(Environment.class);
    private final LifecycleEnvironment lifecycleEnvironment = Mockito.mock(LifecycleEnvironment.class);
    private final Consumer<byte[], byte[]> consumer = Mockito.mock(Consumer.class);
    private final ConsumerRecordProcessor<byte[], byte[]> processor = Mockito.mock(ConsumerRecordProcessor.class);
    private final ScheduledExecutorService executor = Mockito.mock(ScheduledExecutorService.class);

    private final HealthCheckRegistry healthCheckRegistry = Mockito.mock(HealthCheckRegistry.class);

    @Before
    public void setup() throws Exception {
        Mockito.reset(environment, lifecycleEnvironment, consumer, processor, executor, healthCheckRegistry);

        factory = new PollingProcessorFactory();
        factory.setTopics(ImmutableList.of("foo","bar"));

        when(environment.lifecycle()).thenReturn(lifecycleEnvironment);
        when(environment.healthChecks()).thenReturn(healthCheckRegistry);

    }

    public class AnswerWithSelf implements Answer<Object> {
        private final Answer<Object> delegate = new ReturnsEmptyValues();
        private final Class<?> clazz;

        public AnswerWithSelf(Class<?> clazz) {
            this.clazz = clazz;
        }

        public Object answer(InvocationOnMock invocation) throws Throwable {
            Class<?> returnType = invocation.getMethod().getReturnType();
            if (returnType == clazz) {
                return invocation.getMock();
            } else {
                return delegate.answer(invocation);
            }
        }
    }

    @Test
    public void test_build() throws Exception {
        String name = "name";

        // This allows us to mock out the builder.
        final ScheduledExecutorServiceBuilder builder = Mockito.mock(ScheduledExecutorServiceBuilder.class,
                new AnswerWithSelf(ScheduledExecutorServiceBuilder.class));
        when(builder.build()).thenReturn(executor);
        when(lifecycleEnvironment.scheduledExecutorService(name)).thenReturn(builder);

        PollingProcessor<byte[], byte[]> polling = factory.build(environment, consumer, processor, name);

        assertThat(polling).isNotNull();

        ArgumentCaptor<Managed> captor = ArgumentCaptor.forClass(Managed.class);
        verify(lifecycleEnvironment, times(1)).manage(captor.capture());
        assertThat(captor.getAllValues().size(), is(1));
        assertThat(captor.getAllValues()).contains(polling);

    }

    @Test
    public void test_build_with_executor() throws Exception {
        String name = "name";

        PollingProcessor<byte[], byte[]> polling = factory.build(environment, consumer, processor, executor, name);

        assertThat(polling).isNotNull();

        ArgumentCaptor<Managed> captor = ArgumentCaptor.forClass(Managed.class);

        verify(lifecycleEnvironment, times(1)).manage(captor.capture());
        assertThat(captor.getAllValues().size(), is(1));
        assertThat(captor.getAllValues()).contains(polling);
        verify(lifecycleEnvironment).addServerLifecycleListener(polling);
    }

}
