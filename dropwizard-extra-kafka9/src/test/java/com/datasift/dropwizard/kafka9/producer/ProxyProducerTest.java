package com.datasift.dropwizard.kafka9.producer;

import com.datasift.dropwizard.kafka9.producer.ProxyProducer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;


/**
 * Tests {@link ProxyProducer}
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(ProducerRecord.class)
public class ProxyProducerTest {

    private Producer delegate;
    private ProxyProducer producer;

    @Before
    public void setUp() throws Exception {
        delegate = Mockito.mock(Producer.class);
        producer = new ProxyProducer(delegate);
    }

    @Test
    public void close() throws Exception {
        producer.close();

        Mockito.verify(delegate).close();
    }

    @Test
    public void closeWithTimeout() throws Exception {
        long timeout = 0;

        producer.close(timeout, TimeUnit.NANOSECONDS);

        Mockito.verify(delegate).close(timeout, TimeUnit.NANOSECONDS);
    }

    @Test
    public void flush() throws Exception {

        producer.flush();

        Mockito.verify(delegate).flush();
    }

    @Test
    public void metrics() throws Exception {

        Map<MetricName, ? extends Metric> map = new HashMap<>();

        Mockito.when(delegate.metrics()).thenReturn(map);

        assertThat(producer.metrics()).isEqualTo(map);
    }

    @Test
    public void partitionsFor() throws Exception {

        List<PartitionInfo> partitionsFor = new ArrayList<>();

        String topic = "topic";

        Mockito.when(delegate.partitionsFor(topic)).thenReturn(partitionsFor);

        assertThat(producer.partitionsFor(topic)).isEqualTo(partitionsFor);
    }

    @Test
    public void sendNoCallback() throws Exception {

        ProducerRecord record = Mockito.mock(ProducerRecord.class);

        producer.send(record);

        Mockito.verify(delegate).send(record);

    }

    @Test
    public void send() throws Exception {

        ProducerRecord record = Mockito.mock(ProducerRecord.class);
        Callback callback = Mockito.mock(Callback.class);

        producer.send(record, callback);

        Mockito.verify(delegate).send(record, callback);
    }
}