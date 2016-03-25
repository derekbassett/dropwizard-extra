package com.datasift.dropwizard.kafka.consumer;

import com.codahale.metrics.health.HealthCheck;
import org.junit.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests {@link KafkaConsumerHealthCheck}
 */
public class KafkaConsumerHealthCheckTest {

    @Test
    public void check_is_healthy() throws Exception {
        final RunnableProcessor processor = Mockito.mock(RunnableProcessor.class);
        KafkaConsumerHealthCheck healthCheck = new KafkaConsumerHealthCheck(processor);

        Mockito.when(processor.isRunning()).thenReturn(true);

        assertThat(healthCheck.check()).isEqualTo(HealthCheck.Result.healthy());
    }

    @Test
    public void check_is_not_healthy() throws Exception {
        final RunnableProcessor processor = Mockito.mock(RunnableProcessor.class);
        KafkaConsumerHealthCheck healthCheck = new KafkaConsumerHealthCheck(processor);

        Mockito.when(processor.isRunning()).thenReturn(false);

        assertThat(healthCheck.check()).isEqualToComparingFieldByField(HealthCheck.Result.unhealthy("Consumer not consuming any partitions"));
    }

}