package com.datasift.dropwizard.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests {@link ManagedConsumer}
 */
public class ManagedConsumerTest {
    private Consumer<byte[], byte[]> delegate;
    private ManagedConsumer<byte[], byte[]> consumer;

    @Before
    public void setUp() throws Exception {
        delegate = Mockito.mock(Consumer.class);
        consumer = new ManagedConsumer<>(delegate);
    }

    @Test
    public void stop() throws Exception {

        consumer.stop();

        Mockito.verify(delegate).close();
    }

    @Test
    public void assignment() throws Exception {

    }

    @Test
    public void subscription() throws Exception {

    }

    @Test
    public void subscribe() throws Exception {

    }

    @Test
    public void unsubscribe() throws Exception {

    }

    @Test
    public void poll() throws Exception {

    }

    @Test
    public void commitSync() throws Exception {

    }

    @Test
    public void commitAsync() throws Exception {

    }

    @Test
    public void commitAsync1() throws Exception {

    }

    @Test
    public void seek() throws Exception {

    }

    @Test
    public void seekToBeginning() throws Exception {

    }

    @Test
    public void seekToEnd() throws Exception {

    }

    @Test
    public void position() throws Exception {

    }

    @Test
    public void committed() throws Exception {

    }

    @Test
    public void metrics() throws Exception {

    }

    @Test
    public void partitionsFor() throws Exception {

    }

    @Test
    public void listTopics() throws Exception {

    }

    @Test
    public void pause() throws Exception {

    }

    @Test
    public void resume() throws Exception {

    }

    @Test
    public void close() throws Exception {

    }

    @Test
    public void wakeup() throws Exception {

    }

    @Test
    public void commitAsync2() throws Exception {

    }

    @Test
    public void commitSync1() throws Exception {

    }

    @Test
    public void assign() throws Exception {

    }

    @Test
    public void subscribe1() throws Exception {

    }

    @Test
    public void subscribe2() throws Exception {

    }

}