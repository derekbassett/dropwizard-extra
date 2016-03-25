package com.datasift.dropwizard.kafka;

import com.datasift.dropwizard.kafka.consumer.ConsumerRecordProcessor;
import com.datasift.dropwizard.kafka.consumer.InstrumentedConsumer;
import com.datasift.dropwizard.kafka.consumer.KafkaConsumerHealthCheck;
import com.datasift.dropwizard.kafka.consumer.ManagedConsumer;
import com.datasift.dropwizard.kafka.consumer.PollingProcessor;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.dropwizard.setup.Environment;
import io.dropwizard.util.Duration;
import io.dropwizard.util.Size;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

/**
 * A factory for creating and managing {@link Consumer} instances.
 * <p>
 * The {@link Consumer} implementation will be determined by the configuration used to create
 * it.
 * <p>
 * The resultant {@link KafkaConsumer} will have its lifecycle managed by the {@link Environment}
 * and will have {@link com.codahale.metrics.health.HealthCheck}s installed to monitor its status.
 */
public class KafkaConsumerFactory extends KafkaClientFactory {

    // TODO: REMOVE with OffsetResetStrategy
    /**
     * A description of the initial offset to consume from a partition when no committed offset
     * exists.
     * <p>
     * <dl>
     *     <dt>EARLIEST</dt><dd>Use the earliest available offset. In effect,
     *                          consuming the entire log.</dd>
     *     <dt>LATEST</dt><dd>Use the latest available offset. In effect,
     *                         tailing the end of the log.</dd>
     *     <dt>NONE</dt><dd>throw exception to the consumer if no previous offset is found</dd>
     * </dl>
     */
    public enum InitialOffset { EARLIEST, LATEST, NONE }

    @NotEmpty
    protected String group = "";

    @NotNull
    protected ImmutableList<String> topics = ImmutableList.of();

    @NotNull
    protected Duration sessionTimeout = Duration.milliseconds(30000);

    @NotNull
    protected Size receiveBufferSize = Size.kilobytes(64);

    @NotNull
    protected Size minFetch = Size.bytes(1);

    @NotNull
    protected Duration heartbeatInterval = Duration.milliseconds(3000);

    @NotNull
    protected Size maxPartitionFetch = Size.megabytes(1);

    @NotNull
    protected InitialOffset initialOffset = InitialOffset.LATEST;

    @NotNull
    protected Duration connectionsMaxIdle = Duration.milliseconds(540000);

    protected boolean autoCommit = true;



    @NotNull
    protected Duration autoCommitInterval = Duration.seconds(5);




    @NotNull
    protected Duration backOffIncrement = Duration.seconds(1);



    // This defaults to basically waiting for Godot (Forever) with reading the consumer you can of course tweak as you want.
    @NotNull
    protected Duration pollTimeout = Duration.milliseconds(Long.MAX_VALUE);

    @NotNull
    protected Duration initialRecoveryDelay = Duration.milliseconds(500);

    @NotNull
    protected Duration maxRecoveryDelay = Duration.minutes(5);

    @NotNull
    protected Duration retryResetDelay = Duration.minutes(2);

    @Min(-1)
    protected int maxRecoveryAttempts = 20;

    @NotNull
    protected boolean shutdownOnFatal = false;

    @NotNull
    protected Duration shutdownGracePeriod = Duration.seconds(5);

    @NotNull
    protected Duration startDelay = Duration.seconds(2);

    /**
     * Returns the consumer group the {@link KafkaConsumer} belongs to.
     *
     * @return the consumer group the {@link KafkaConsumer} belongs to.
     */
    @JsonProperty
    public String getGroup() {
        return group;
    }

    /**
     * Sets the consumer group the {@link KafkaConsumer} belongs to.
     *
     * @param group the consumer group the {@link KafkaConsumer} belongs to.
     */
    @JsonProperty
    public void setGroup(final String group) {
        this.group = group;
    }

    /**
     * Returns a listing of the topics to consume from.
     * <p>
     * Topics not referenced will not be consumed from.
     *
     * @return a List of topics.
     */
    @JsonProperty
    public ImmutableList<String> getTopics() {
        return topics;
    }

    /**
     * Sets a list of topics to consume from.
     * <p>
     * Topics not referenced will not be consumed from.
     *
     * @param topics a List of topics.
     */
    @JsonProperty
    public void setTopics(final ImmutableList<String> topics) {
        this.topics = topics;
    }

    /**
     * Returns the time the Consumer should wait to receive messages before timing out
     * the stream.
     * <p>
     *
     * @return the maximum time to wait when receiving messages from a broker before timing out.
     *
     */
    @JsonProperty
    public Duration getSessionTimeout() {
        return sessionTimeout;
    }

    /**
     * Sets the time the {@link Consumer} should wait to receive messages before timing out
     * the stream.
     * <p>
     *
     * @param sessionTimeout the maximum time to wait when receiving messages before timing out.
     *
     */
    @JsonProperty
    public void setSessionTimeout(final Duration sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
    }

    /**
     * Returns the size of the client-side receive buffer.
     *
     * @return the size of the client-side receive buffer.
     */
    @JsonProperty
    public Size getReceiveBufferSize() {
        return receiveBufferSize;
    }

    /**
     * Sets the size of the client-side receive buffer.
     *
     * @param size the size of the client-side receive buffer.
     */
    @JsonProperty
    public void setReceiveBufferSize(final Size size) {
        this.receiveBufferSize = size;
    }

    /**
     * Returns the minimum size of data the server should return for a fetch request.
     *
     * @return the minimum size of data the server should return for a fetch request.
     *
     */
    @JsonProperty
    public Size getMinFetchSize() {
        return minFetch;
    }

    /**
     * Sets the minimum size of data the server should return for a fetch request.
     * <p>
     * @param size the minimum size of data the server should return for a fetch request.
     *
     */
    @JsonProperty
    public void setMinFetchSize(final Size size) {
        this.minFetch = size;
    }

    /**
     * Returns the heartbeat interval
     * @return the expected time between heartbeats
     */
    @JsonProperty
    public Duration getHeartbeatInterval() {
        return heartbeatInterval;
    }

    /**
     * Sets the expected time between heartbeats
     * @param heartbeatInterval
     */
    @JsonProperty
    public void setHeartbeatInterval(Duration heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
    }

    @JsonProperty
    public Size getMaxPartitionFetch() {
        return maxPartitionFetch;
    }

    @JsonProperty
    public void setMaxPartitionFetch(Size maxPartitionFetch) {
        this.maxPartitionFetch = maxPartitionFetch;
    }

    // SSL HERE

    /**
     * Returns the setting for the initial offset to consume from when no committed offset exists.
     *
     * @return the initial offset to consume from in a partition.
     *
     * @see InitialOffset
     */
    @JsonProperty
    public InitialOffset getInitialOffset() {
        return initialOffset;
    }

    /**
     * Sets the setting for the initial offset to consume from when no committed offset exists.
     *
     * @param initialOffset the initial offset to consume from in a partition.
     *
     * @see InitialOffset
     */
    @JsonProperty
    public void setInitialOffset(final InitialOffset initialOffset) {
        this.initialOffset = initialOffset;
    }

    @JsonProperty
    public Duration getConnectionsMaxIdle() {
        return connectionsMaxIdle;
    }

    @JsonProperty
    public void setConnectionsMaxIdle(Duration connectionsMaxIdle) {
        this.connectionsMaxIdle = connectionsMaxIdle;
    }

    /**
     * Returns whether to automatically commit the offsets that have been consumed.
     *
     * @return true to commit the last consumed offset periodically; false to never commit offsets.
     *
     * @see #getAutoCommitInterval
     */
    @JsonProperty
    public boolean getAutoCommit() {
        return autoCommit;
    }

    /**
     * Sets whether to automatically commit the offsets that have been consumed.
     *
     * @param autoCommit true to commit the last consumed offset periodically;
     *                   false to never commit offsets.
     *
     * @see #getAutoCommitInterval
     */
    @JsonProperty
    public void setAutoCommit(final boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    /**
     * Returns the cumulative delay before polling a broker again when no data is returned.
     * <p>
     * When fetching data from a broker, if there is no new data, there will be a delay before
     * polling the broker again. This controls the duration of the delay by increasing it linearly,
     * on each poll attempt.
     *
     * @return the amount by which the retry sessionTimeout will be increased after each attempt.
     */
    @JsonProperty
    public Duration getBackOffIncrement() {
        return backOffIncrement;
    }

    /**
     * Sets the cumulative delay before polling a broker again when no data is returned.
     * <p>
     * When fetching data from a broker, if there is no new data, there will be a delay before
     * polling the broker again. This controls the duration of the delay by increasing it linearly,
     * on each poll attempt.
     *
     * @param increment the amount by which the retry sessionTimeout will be increased after each attempt.
     */
    @JsonProperty
    public void setBackOffIncrement(final Duration increment) {
        this.backOffIncrement = increment;
    }



    /**
     * Gets the frequency to automatically commit previously consumed offsets, if enabled.
     *
     * @return the frequency to automatically commit the previously consumed offsets, when enabled.
     *
     * @see #getAutoCommit
     */
    @JsonProperty
    public Duration getAutoCommitInterval() {
        return autoCommitInterval;
    }


    /**
     * Sets the frequency to automatically commit previously consumed offsets, if enabled.
     * 
     * @param autoCommitInterval the frequency with which to auto commit.
     *
     * @see #getAutoCommit
     */
    @JsonProperty
    public void setAutoCommitInterval(final Duration autoCommitInterval) {
        this.autoCommitInterval = autoCommitInterval;
    }

    @JsonProperty
    public Duration getPollTimeout() { return pollTimeout; }

    @JsonProperty
    public void setPollTimeout(final Duration pollTimeout) {
        this.pollTimeout = pollTimeout;
    }

    @JsonProperty
    public Duration getInitialRecoveryDelay() {
        return initialRecoveryDelay;
    }

    @JsonProperty
    public void setInitialRecoveryDelay(final Duration initialRecoveryDelay) {
        this.initialRecoveryDelay = initialRecoveryDelay;
    }

    @JsonProperty
    public Duration getMaxRecoveryDelay() {
        return maxRecoveryDelay;
    }

    @JsonProperty
    public void setMaxRecoveryDelay(final Duration maxRecoveryDelay) {
        this.maxRecoveryDelay = maxRecoveryDelay;
    }

    @JsonProperty
    public Duration getRetryResetDelay() {
        return retryResetDelay;
    }

    @JsonProperty
    public void setRetryResetDelay(final Duration retryResetDelay) {
        this.retryResetDelay = retryResetDelay;
    }

    @JsonProperty
    public int getMaxRecoveryAttempts() {
        return maxRecoveryAttempts;
    }

    @JsonProperty
    public void setMaxRecoveryAttempts(final int maxRecoveryAttempts) {
        this.maxRecoveryAttempts = maxRecoveryAttempts;
    }

    @JsonProperty
    public boolean isShutdownOnFatal() {
        return shutdownOnFatal;
    }

    @JsonProperty
    public void setShutdownOnFatal(final boolean shutdownOnFatal) {
        this.shutdownOnFatal = shutdownOnFatal;
    }

    @JsonProperty
    public Duration getShutdownGracePeriod() {
        return shutdownGracePeriod;
    }

    @JsonProperty
    public void setShutdownGracePeriod(final Duration shutdownGracePeriod) {
        this.shutdownGracePeriod = shutdownGracePeriod;
    }

    @JsonProperty
    public Duration getStartDelay() { return startDelay; }

    @JsonProperty
    public void setStartDelay(final Duration startDelay) {
        this.startDelay = startDelay;
    }

    protected <K, V> KafkaConsumer buildUnmanagedConsumer(Class<? extends Deserializer<K>> keyDeserializer,
                                                          Class<? extends Deserializer<V>> valueDeserializer,
                                                          String name) {
        return new KafkaConsumer(
                toProperties(this, valueDeserializer, keyDeserializer, name));
    }

    public <V> Consumer<?, V> build(final Class<? extends Deserializer<V>> valueDeserializer,
                                    final Environment environment,
                                    final String name) {
        return build(ByteArrayDeserializer.class, valueDeserializer, environment, name);
    }

    public <K, V> Consumer<K, V> build(final Class<? extends Deserializer<K>> keyDeserializer,
                                       final Class<? extends Deserializer<V>> valueDeserializer,
                                       final Environment environment,
                                       final String name) {
        final KafkaConsumer<K, V> kafka = buildUnmanagedConsumer(keyDeserializer, valueDeserializer, name);
        final ManagedConsumer<K, V> managed = new ManagedConsumer<>(kafka);

        environment.lifecycle().manage(managed);

        return new InstrumentedConsumer<>(managed, environment.metrics(), name);
    }

    /**
     * Prepares a {@link PollingProcessorBuilder} for a given {@link ConsumerRecordProcessor}.
     *
     * @param processor the {@link ConsumerRecordProcessor} to process the stream with.
     * @return a {@link PollingProcessorBuilder} to build a {@link KafkaConsumer} for the given
     *         processor.
     */
    public PollingProcessorBuilder<byte[], byte[]> processWith(final ConsumerRecordProcessor<byte[], byte[]> processor) {
        return processWith(ByteArrayDeserializer.class, processor);
    }

    /**
     * Prepares a {@link PollingProcessorBuilder} for a given {@link Deserializer} and {@link
     * ConsumerRecordProcessor}.
     * <p>
     * The decoder instance is used to decode Messages in the stream before being passed to
     * the processor.
     *
     * @param deserializer the {@link Deserializer} instance to deserialize values with
     * @param processor a {@link ConsumerRecordProcessor} to process the message stream
     * @return a {@link PollingProcessorBuilder} to build a {@link PollingProcessor} for the given
     *         processor and decoder.
     */
    public <V> PollingProcessorBuilder<byte[], V> processWith(final Class<? extends Deserializer<V>> deserializer,
                                                              final ConsumerRecordProcessor<byte[], V> processor) {
        return new PollingProcessorBuilder<>(ByteArrayDeserializer.class, deserializer, processor);
    }

    public <K, V> PollingProcessorBuilder<K, V> processWith(final Class<? extends Deserializer<K>> keyDeserializer,
                                                            final Class<? extends Deserializer<V>> valueDeserializer,
                                                            final ConsumerRecordProcessor<K, V> processor) {
        return new PollingProcessorBuilder<>(keyDeserializer, valueDeserializer, processor);
    }

    /**
     * A Builder for building a configured {@link PollingProcessor}.
     *
     * @param <V> the type of the messages the {@link PollingProcessor} will process.
     */
    public class PollingProcessorBuilder<K, V> {

        private final Class<? extends Deserializer<K>> keyDeserializer;
        private final Class<? extends Deserializer<V>> valueDeserializer;
        private final ConsumerRecordProcessor<K, V> processor;
        private boolean autoCommit = true;
        private int batchCount;
        private static final String DEFAULT_NAME = "kafka-consumer-default";

        private PollingProcessorBuilder(final Class<? extends Deserializer<K>> keyDeserializer,
                                        final Class<? extends Deserializer<V>> valueDeserializer,
                                        final ConsumerRecordProcessor<K, V> processor) {
            this.keyDeserializer = keyDeserializer;
            this.valueDeserializer = valueDeserializer;
            this.processor = processor;
        }

        /**
         * Builds a {@link PollingProcessor} instance for the given {@link Environment}.
         *
         * @param environment the {@link Environment} to build {@link PollingProcessor} instances for.
         *
         * @return a managed and configured {@link PollingProcessor}.
         */
        public PollingProcessor<K, V> build(final Environment environment) {
            return build(environment, DEFAULT_NAME);
        }

        /**
         * Builds a {@link PollingProcessor} instance from the given {@link ExecutorService} and name,
         * for the given {@link Environment}.
         * <p>
         * The name is used to identify the returned {@link PollingProcessor} instance, for example, as
         * the name of its {@link com.codahale.metrics.health.HealthCheck}s, thread pool, etc.
         * <p>
         * This implementation creates a new {@link ExecutorService} with a fixed-size thread-pool,
         * configured for one thread per-partition the {@link PollingProcessor} is being configured to
         * consume.
         *
         * @param environment the {@link Environment} to build {@link PollingProcessor} instances for.
         * @param name the name of the {@link PollingProcessor}.
         *
         * @return a managed and configured {@link PollingProcessor}.
         */
        public PollingProcessor<K, V> build(final Environment environment, final String name) {

            final ScheduledExecutorService executor = environment.lifecycle()
                    .scheduledExecutorService(name)
                        .threads(1)
                        .shutdownTime(getShutdownGracePeriod())
                        .build();

            return build(environment, executor, name);
        }

        /**
         * Builds a {@link PollingProcessor} instance from the given {@link ExecutorService} and name,
         * for the given {@link Environment}.
         * <p>
         * The name is used to identify the returned {@link PollingProcessor} instance, for example, as
         * the name of its {@link com.codahale.metrics.health.HealthCheck}s, etc.
         *
         * @param environment the {@link Environment} to build {@link PollingProcessor} instances for.
         * @param executor the {@link ExecutorService} to process messages with.
         * @param name the name of the {@link PollingProcessor}.
         *
         * @return a managed and configured {@link PollingProcessor}.
         */
        public PollingProcessor<K, V> build(final Environment environment,
                                             final ScheduledExecutorService executor,
                                             final String name) {
            final Consumer<K, V> consumer = KafkaConsumerFactory.this.
                    build(keyDeserializer, valueDeserializer, environment, name);
            final PollingProcessor<K, V> processor = build(consumer, executor);

            // manage the consumer
            environment.lifecycle().manage(processor);
            environment.lifecycle().addServerLifecycleListener(processor);

            // add health checks
            environment.healthChecks().register(name, new KafkaConsumerHealthCheck(processor));

            return processor;
        }

        /**
         * Builds a {@link PollingProcessor} instance with this builders' configuration using the
         * given {@link ExecutorService}.
         * <p>
         * If possible, it's always preferable to use one of the overloads that take an {@link
         * Environment} directly. This overload exists for situations where you don't have access to
         * an {@link Environment} (e.g. some Commands or unit tests).
         *
         * @param executor The {@link ExecutorService} to process messages with.
         *
         * @return a configured {@link PollingProcessor}.
         */
        private PollingProcessor<K, V> build(final Consumer<K, V> consumer, final ScheduledExecutorService executor) {
            final PollingProcessor<K, V> poller = new PollingProcessor<>(
                    consumer,
                    getTopics(),
                    processor,
                    executor,
                    getPollTimeout(),
                    isShutdownOnFatal(),
                    getStartDelay(),
                    autoCommit,
                    batchCount
            );
            return poller;
        }
    }

    static <K, V> Properties toProperties(final KafkaConsumerFactory factory,
                                          final Class<? extends Deserializer<V>> valueDeserializer,
                                          final Class<? extends Deserializer<K>> keyDeserializer,
                                          final String name) {
        final Properties properties = new Properties();

        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, factory.buildBrokerProperty());

        if(keyDeserializer != null) {
            properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getCanonicalName());
        }
        properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getCanonicalName());
        properties.setProperty(FETCH_MIN_BYTES_CONFIG, String.valueOf(factory.getMinFetchSize().toBytes()));
        properties.setProperty(GROUP_ID_CONFIG, factory.getGroup());
        properties.setProperty(HEARTBEAT_INTERVAL_MS_CONFIG, String.valueOf(factory.getHeartbeatInterval().toMilliseconds()));
        properties.setProperty(MAX_PARTITION_FETCH_BYTES_CONFIG, String.valueOf(factory.getMaxPartitionFetch().toBytes()));
        properties.setProperty(SESSION_TIMEOUT_MS_CONFIG, String.valueOf(factory.getSessionTimeout().toMilliseconds()));

        // TODO: ADD SSL

        properties.setProperty(AUTO_OFFSET_RESET_CONFIG, String.valueOf(factory.getInitialOffset()).toLowerCase());
        properties.setProperty(CONNECTIONS_MAX_IDLE_MS_CONFIG, String.valueOf(factory.getConnectionsMaxIdle().toMilliseconds()));
        properties.setProperty(ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(factory.getAutoCommit()));
        properties.setProperty(AUTO_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(factory.getAutoCommitInterval().toMilliseconds()));

        properties.setProperty(CLIENT_ID_CONFIG, factory.buildClientIdProperty(name));
        properties.setProperty(FETCH_MAX_WAIT_MS_CONFIG, String.valueOf(factory.getBackOffIncrement().toMilliseconds()));

        return properties;
    }
}
