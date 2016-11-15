package com.datasift.dropwizard.kafka9.producer;

import com.datasift.dropwizard.kafka9.producer.ManagedProducer;
import org.apache.kafka.clients.producer.Producer;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests {@link ManagedProducer}
 */
public class ManagedProducerTest {


    @Test
    public void stop() throws Exception {
        Producer delegate = Mockito.mock(Producer.class);
        ManagedProducer producer = new ManagedProducer(delegate);

        producer.stop();

        Mockito.verify(delegate).close();
    }
}