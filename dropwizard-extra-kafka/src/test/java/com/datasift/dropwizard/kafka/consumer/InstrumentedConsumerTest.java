package com.datasift.dropwizard.kafka.consumer;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests {@link InstrumentedConsumer}
 */
public class InstrumentedConsumerTest {


    @Test
    public void poll() throws Exception {
        Consumer<byte[], byte[]> underlying = Mockito.mock(Consumer.class);
        String name = "metrics";
        MetricRegistry registry = new MetricRegistry();
        Meter meter = registry.meter(MetricRegistry.name(name, "received"));
        long timeout = 0;
        int count = 0;

        assertThat(meter.getCount()).isEqualTo(count);

        count = 5;
        ConsumerRecords records = Mockito.mock(ConsumerRecords.class);
        Mockito.when(records.count()).thenReturn(count);
        Mockito.when(underlying.poll(timeout)).thenReturn(records);

        InstrumentedConsumer<byte[], byte[]> consumer = new InstrumentedConsumer<>(underlying, registry, name);

        assertThat(consumer.poll(0)).isEqualTo(records);
        assertThat(meter.getCount()).isEqualTo(count);
    }

}