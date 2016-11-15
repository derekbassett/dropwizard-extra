package com.datasift.dropwizard.kafka9.consumer;

import com.datasift.dropwizard.kafka9.consumer.ConsumerRecordProcessor;
import com.datasift.dropwizard.kafka9.consumer.PollingProcessor;
import com.google.common.collect.ImmutableList;
import io.dropwizard.util.Duration;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.util.component.AbstractLifeCycle;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest(AbstractLifeCycle.class)
@PowerMockIgnore("javax.management.*")
public class PollingProcessorTest {

    private List<String> topics;
    private Duration pollTimeout;
    private Duration startDelay;
    private boolean autoCommit;
    private int batchSize;
    private ScheduledExecutorService executor;

    @Before
    public void setUp() throws Exception {
        topics = ImmutableList.of("foo", "bar");
        pollTimeout = Duration.milliseconds(50);
        startDelay = Duration.milliseconds(0);
        autoCommit = true;
        batchSize = 25;
        executor = Mockito.mock(ScheduledExecutorService.class);

        when(executor.isTerminated()).thenReturn(false);
        when(executor.isShutdown()).thenReturn(false);

        //Mock an Executor to simply run the BasicPollLoop
        Mockito.doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] args = invocationOnMock.getArguments();
                Runnable runnable = (Runnable)args[0];
                runnable.run();
                return null;
            }
        }).when(executor).schedule(Mockito.any(Runnable.class), anyLong(), Mockito.any(TimeUnit.class));
    }

    @Test
    public void shouldNotBeRunningAfterUnrecoverableException() throws Exception {

        Server jettyServer = PowerMockito.mock(Server.class);
        PowerMockito.doNothing().when(jettyServer).stop();

        final Consumer<byte[], byte[]> consumer = Mockito.mock(Consumer.class);
        Mockito.when(consumer.poll(pollTimeout.toMilliseconds()))
                .thenThrow(new IllegalStateException());

        final ConsumerRecordProcessor consumerRecordProcessor = Mockito.mock(ConsumerRecordProcessor.class);

        final PollingProcessor processor = new PollingProcessor(
                consumer,
                topics,
                consumerRecordProcessor,
                executor,
                pollTimeout,
                false,
                startDelay,
                autoCommit,
                batchSize);

        processor.serverStarted(jettyServer);
        assertThat(processor.isRunning()).isTrue();
        processor.start();
        assertThat(processor.isRunning()).isFalse();

        Mockito.verify(jettyServer, timeout(1000).never()).stop();
        Mockito.verify(consumer).close();
    }

    @Test
    public void shouldStillBeRunningAfterRecoverableException() throws Exception {

        Server jettyServer = PowerMockito.mock(Server.class);
        Mockito.doNothing().when(jettyServer).stop();

        final Consumer<byte[], byte[]> consumer = Mockito.mock(Consumer.class);
        final ConsumerRecords<byte[], byte[]> records = ConsumerRecords.empty();


        final ConsumerRecordProcessor consumerRecordProcessor = Mockito.mock(ConsumerRecordProcessor.class);

        final PollingProcessor processor = new PollingProcessor(
                consumer,
                topics,
                consumerRecordProcessor,
                executor,
                pollTimeout,
                false,
                startDelay,
                autoCommit,
                batchSize);

        Mockito.when(consumer.poll(pollTimeout.toMilliseconds()))
                .thenThrow(new RuntimeException())
                .thenAnswer(new Answer() {

                    @Override
                    public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                        assertThat(processor.isRunning()).isTrue();
                        throw new IllegalStateException();
                    }
                });

        processor.serverStarted(jettyServer);
        assertThat(processor.isRunning()).isTrue();
        processor.start();

        Mockito.verify(jettyServer, timeout(1000).never()).stop();
    }

    @Test
    public void testServerShutDownOnUnrecoverableException() throws Exception {

        Server jettyServer = PowerMockito.mock(Server.class);
        Mockito.doNothing().when(jettyServer).stop();

        //Mock the StreamProcessor - Throws Unrecoverable IllegalStateException
        final Consumer<byte[], byte[]> consumer = Mockito.mock(Consumer.class);
        Mockito.doThrow(new IllegalStateException()).when(consumer).poll(pollTimeout.toMilliseconds());

        final ConsumerRecordProcessor consumerRecordProcessor = Mockito.mock(ConsumerRecordProcessor.class);
        final boolean shutDownServerOnUnrecoverableError = true;

        final PollingProcessor processor = new PollingProcessor(
                consumer,
                topics,
                consumerRecordProcessor,
                executor,
                pollTimeout,
                shutDownServerOnUnrecoverableError,
                startDelay,
                autoCommit,
                batchSize);
        processor.serverStarted(jettyServer);
        assertThat(processor.isRunning()).isTrue();
        processor.start();
        assertThat(processor.isRunning()).isFalse();

        Mockito.verify(jettyServer, timeout(1000).times(1)).stop();
    }

}