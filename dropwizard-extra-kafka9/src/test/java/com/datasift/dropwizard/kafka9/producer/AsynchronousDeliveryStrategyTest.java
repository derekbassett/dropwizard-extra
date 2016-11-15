package com.datasift.dropwizard.kafka9.producer;

import com.datasift.dropwizard.kafka9.producer.AsynchronousDeliveryStrategy;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.concurrent.Executor;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 *
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ProducerRecord.class})
public class AsynchronousDeliveryStrategyTest {

    private Executor executor;
    private AsynchronousDeliveryStrategy strategy;
    private Producer<String, String> producer;
    private ProducerRecord<String, String> record;
    private Callback callback;

    @Before
    public void setUp() throws Exception {
        executor = mock(Executor.class);
        strategy = new AsynchronousDeliveryStrategy(executor);
        producer = mock(Producer.class);
        record = mock(ProducerRecord.class);
        callback = mock(Callback.class);
    }

    @Test
    public void should_send_asynchronously() throws Exception {

        strategy.send(producer, record, callback);

        ArgumentCaptor<Runnable> captor = ArgumentCaptor.forClass(Runnable.class);

        verify(executor).execute(captor.capture());

        captor.getValue().run();
        verify(producer).send(record, callback);
    }

    @Test
    public void should_send_asynchronously_with_kafka_exception() throws Exception {

        strategy.send(producer, record, callback);

        KafkaException exception = new KafkaException();
        doThrow(exception).when(producer).send(record, callback);

        ArgumentCaptor<Runnable> captor = ArgumentCaptor.forClass(Runnable.class);

        verify(executor).execute(captor.capture());

        captor.getValue().run();
        verify(producer).send(record, callback);
        verify(callback).onCompletion(null, exception);
    }
}