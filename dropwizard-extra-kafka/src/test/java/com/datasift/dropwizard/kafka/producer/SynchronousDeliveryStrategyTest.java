package com.datasift.dropwizard.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.doThrow;
import static org.powermock.api.mockito.PowerMockito.mock;

/**
 *
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ProducerRecord.class})
public class SynchronousDeliveryStrategyTest {

    private SynchronousDeliveryStrategy strategy;
    private Producer<String, String> producer;
    private ProducerRecord<String, String> record;
    private Callback callback;

    @Before
    public void setUp() throws Exception {
        strategy = new SynchronousDeliveryStrategy();
        producer = mock(Producer.class);
        record = mock(ProducerRecord.class);
        callback = mock(Callback.class);
    }

    @Test
    public void should_send_asynchronously() throws Exception {

        strategy.send(producer, record, callback);

        verify(producer).send(record, callback);
    }

    @Test
    public void should_send_asynchronously_with_kafka_exception() throws Exception {

        KafkaException exception = new KafkaException();
        doThrow(exception).when(producer).send(record, callback);

        strategy.send(producer, record, callback);

        verify(producer).send(record, callback);
        verify(callback).onCompletion(null, exception);
    }
}