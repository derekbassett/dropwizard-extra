package com.datasift.dropwizard.kafka.consumer;

import io.dropwizard.lifecycle.Managed;
import io.dropwizard.lifecycle.ServerLifecycleListener;
import io.dropwizard.util.Duration;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.eclipse.jetty.server.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * A {@link PollingProcessor} that processes messages synchronously using an {@link ExecutorService}.
 */
public class PollingProcessor<K, V> implements RunnableProcessor, Managed, ServerLifecycleListener {

    private final Logger LOG = LoggerFactory.getLogger(getClass());

    private final Consumer<K, V> consumer;
    private final List<String> topics;
    private final ConsumerRecordProcessor<K, V> processor;
    private final ScheduledExecutorService executor;
    private final Duration pollTimeout;
    private final boolean shutdownOnFatal;
    private final Duration startDelay;
    private final boolean autoCommitEnabled;
    private final int batchCount;

    private Server server = null;
    private ExceptionHandler exceptionHandler;
    private boolean requestShutdown = false;

    // a thread to asynchronously handle unrecoverable errors in the stream consumer
    private final Thread shutdownThread = new Thread("kafka-unrecoverable-error-handler"){
        public void run() {
            while (true) {
                try {
                    Thread.sleep(10000);
                } catch (final InterruptedException e) {
                    // stop sleeping
                }
                if (requestShutdown) {
                    try {
                        if (shutdownOnFatal && server != null) {
                            // shutdown the full service
                            // note: shuts down the consumer as it's Managed by the Environment
                            server.stop();
                        } else {
                            // just shutdown the consumer
                            PollingProcessor.this.stop();
                        }
                    } catch (Exception e) {
                        LOG.error("Error occurred while attempting emergency shut down.", e);
                    }
                }
            }
        }
    };

    /**
     * Creates a {@link PollingProcessor} to process a stream.
     *
     * @param consumer a {@link Consumer} for consuming messages on.
     * @param processor a {@link ConsumerRecordProcessor} for processing messages.
     * @param executor the {@link ExecutorService} to process the stream with.
     * @param pollTimeout the time we wait when polling Kafka.
     * @param shutdownOnFatal booelan stating whether or not to shut down on fatal error
     * @param startDelay the amount of time to delay at start
     */
    public PollingProcessor(final Consumer<K, V> consumer,
                            final List<String> topics,
                            final ConsumerRecordProcessor<K, V> processor,
                            final ScheduledExecutorService executor,
                            final Duration pollTimeout,
                            final boolean shutdownOnFatal,
                            final Duration startDelay,
                            final boolean autoCommitEnabled,
                            final int batchCount) {
        this.consumer = consumer;
        this.topics = topics;
        this.processor = processor;
        this.executor = executor;
        this.pollTimeout = pollTimeout;
        this.shutdownOnFatal = shutdownOnFatal;
        this.startDelay = startDelay;
        this.autoCommitEnabled = autoCommitEnabled;
        this.batchCount = batchCount;
        this.exceptionHandler = new LoggingExceptionHandler<>(LOG, this);

        shutdownThread.setDaemon(true);
        shutdownThread.start();
    }

    @Override
    public void serverStarted(final Server server) {
        this.server = server;
    }

    @Override
    public void start() throws Exception {
        LOG.info("Starting Kafka consumer");
        executor.schedule(
                new BasicPollLoop(pollTimeout, topics, consumer),
                startDelay.getQuantity(),
                startDelay.getUnit());
    }

    /**
     * Stops this {@link PollingProcessor} immediately.
     *
     * @throws Exception if an error occurs on stop
     */
    @Override
    public void stop() throws Exception {
        // This breaks the poll in case we've set it for forever.
        consumer.wakeup();
    }

    /**
     * Determines if this {@link Consumer} is currently consuming.
     *
     * @return true if this {@link Consumer} is currently consuming from at least one
     *         partition; otherwise, false.
     */
    @Override
    public boolean isRunning() {
        return !executor.isShutdown() && !executor.isTerminated() && !requestShutdown;
    }

    public boolean isAutoCommitEnabled() {
        return autoCommitEnabled;
    }

    public int getBatchCount() {
        return batchCount;
    }

    public ExceptionHandler getExceptionHandler() {
        return exceptionHandler;
    }

    public void setExceptionHandler(ExceptionHandler exceptionHandler) {
        this.exceptionHandler = exceptionHandler;
    }

    @Override
    public void requestShutdown() {
        this.requestShutdown = true;
        this.shutdownThread.interrupt();
    }

    /**
     *
     * The configured {@link BasicPollLoop} is used to process the stream.
     */
    private class BasicPollLoop implements Runnable {

        private final String threadId;
        private final Duration timeout;
        private final List<String> topics;
        private final Consumer<K, V> consumer;

        /**
         * Creates a {@link BasicPollLoop} for the given topic and stream.
         *
         * @param timeout the time we wait for poll to return
         * @param topics the topics we are subscribed for
         * @param consumer the Consumer we are consuming from
         */
        public BasicPollLoop(final Duration timeout, final List<String> topics, final Consumer<K, V> consumer) {
            this.threadId = Thread.currentThread().getName();
            this.timeout = timeout;
            this.topics = topics;
            this.consumer = consumer;
        }

        /**
         * Process the stream using the configured {@link ConsumerRecordProcessor}.
         * <p>
         * If an {@link Exception} is thrown during processing, if it is deemed <i>recoverable</i>,
         * the stream will continue to be consumed.
         * <p>
         * Unrecoverable {@link Exception}s will cause the consumer to shut down completely.
         */
        @Override
        public void run() {
            try {
                LOG.debug("Subscribing {} to topics {}", threadId, topics);
                consumer.subscribe(topics);
                int processed = 0;
                while(isRunning()) {
                    try {
                        ConsumerRecords<K, V> records = consumer.poll(timeout.toMilliseconds());
                        try {
                            processor.process(records);
                        } catch (Exception e) {
                            getExceptionHandler().handleException("Error during processing", e);
                        }
                        processed += records.count();
                        if (!isAutoCommitEnabled()) {
                            if (processed >= getBatchCount()) {
                                consumer.commitSync();
                                processed = 0;
                            }
                        }
                    } catch (final Throwable e) {
                        getExceptionHandler().handleException("Error consuming " + threadId + " from kafka topic", e);
                    }
                }
                LOG.debug("Unsubscribing {} from topics {}", threadId, topics);
                consumer.unsubscribe();
            } catch (final Throwable e) {
                getExceptionHandler().handleException("Error consuming " + threadId + " from kafka topic", e);
            } finally {
                consumer.close();
            }
        }
    }
}
