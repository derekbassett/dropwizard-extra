package com.datasift.dropwizard.kafka;

import com.datasift.dropwizard.kafka.consumer.InstrumentedConsumer;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.setup.Environment;
import io.dropwizard.util.Duration;
import io.dropwizard.util.Size;
import io.dropwizard.validation.MinDuration;
import io.dropwizard.validation.MinSize;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.NotNull;
import java.util.Properties;

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

    @MinSize(1)
    protected Size minFetch = Size.bytes(1);

    @NotEmpty
    protected String group = "";


    @MinDuration(0)
    protected Duration heartbeatInterval = Duration.milliseconds(3000);

    @MinSize(1)
    protected Size maxPartitionFetch = Size.megabytes(1);

    @MinDuration(0)
    protected Duration sessionTimeout = Duration.milliseconds(30000);

    @NotNull
    protected OffsetResetStrategy autoOffsetReset = OffsetResetStrategy.LATEST;

    @MinDuration(0)
    protected Duration connectionsMaxIdle = Duration.milliseconds(540000);

    protected boolean autoCommit = true;

    @MinSize(1)
    protected Size receiveBufferSize = Size.kilobytes(32);

    @MinSize(1)
    protected Size sendBufferSize = Size.kilobytes(128);


    @NotNull
    protected Duration autoCommitInterval = Duration.seconds(5);



    @NotNull
    protected Duration maxFetchWaitInterval = Duration.seconds(1);

    @NotNull
    protected Duration reconnectBackoffInterval = Duration.milliseconds(50);

    @NotNull
    protected Duration retryBackoffInterval = Duration.milliseconds(100);

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
     * Returns the heartbeat interval
     *
     * @return the expected time between heartbeats
     */
    @JsonProperty
    public Duration getHeartbeatInterval() {
        return heartbeatInterval;
    }

    /**
     * Sets the expected time between heartbeats
     *
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
     * @see OffsetResetStrategy
     */
    @JsonProperty
    public OffsetResetStrategy getAutoOffsetReset() {
        return autoOffsetReset;
    }

    /**
     * Sets the setting for the initial offset to consume from when no committed offset exists.
     *
     * @param autoOffsetReset the initial offset to consume from in a partition.
     *
     * @see OffsetResetStrategy
     */
    @JsonProperty
    public void setAutoOffsetReset(final OffsetResetStrategy autoOffsetReset) {
        this.autoOffsetReset = autoOffsetReset;
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
     * Returns the size of the client-side send buffer.
     *
     * @return the size of the client-side send buffer.
     */
    @JsonProperty
    public Size getSendBufferSize() {
        return sendBufferSize;
    }

    /**
     * Sets the size of the client-side send buffer.
     *
     * @param size the size of the client-side send buffer.
     */
    @JsonProperty
    public void setSendBufferSize(final Size size) {
        this.sendBufferSize = size;
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
    public Duration getMaxFetchWaitInterval() {
        return maxFetchWaitInterval;
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
    public void setMaxFetchWaitInterval(final Duration increment) {
        this.maxFetchWaitInterval = increment;
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
    public Duration getReconnectBackoffInterval() {
        return reconnectBackoffInterval;
    }

    @JsonProperty
    public void setReconnectBackoffInterval(Duration reconnectBackoffInterval) {
        this.reconnectBackoffInterval = reconnectBackoffInterval;
    }

    @JsonProperty
    public Duration getRetryBackoffInterval() {
        return retryBackoffInterval;
    }

    @JsonProperty
    public void setRetryBackoffInterval(Duration retryBackoffInterval) {
        this.retryBackoffInterval = retryBackoffInterval;
    }

    public <V> Consumer<byte[], V> build(final Environment environment, final Deserializer<V> valueDeserializer, final String name) {
        return build(environment, valueDeserializer, new ByteArrayDeserializer(), name);
    }

    public <V> Consumer<byte[], V> build(final Environment environment, final Class<? extends Deserializer<V>> valueDeserializer, final String name) {
        return build(environment, valueDeserializer, ByteArrayDeserializer.class, name);
    }

    public <K, V> Consumer<K, V> build(final Environment environment, final Class<? extends Deserializer<V>> valueDeserializer, final Class<? extends Deserializer<K>> keyDeserializer,
                                       final String name) {
        final Consumer<K, V> kafka = buildUnmanaged(keyDeserializer, valueDeserializer, name);
        return new InstrumentedConsumer<>(kafka, environment.metrics(), name);
    }

    public <K, V> Consumer<K, V> build(final Environment environment, final Deserializer<V> valueDeserializer, final Deserializer<K> keyDeserializer,
                                       final String name) {
        final Consumer<K, V> kafka = buildUnmanaged(keyDeserializer, valueDeserializer, name);
        return new InstrumentedConsumer<>(kafka, environment.metrics(), name);
    }

    protected <K, V> Consumer<K, V> buildUnmanaged(final Class<? extends Deserializer<K>> keyDeserializer,
                                                   final Class<? extends Deserializer<V>> valueDeserializer,
                                                   final String name) {
        return buildUnmanaged(toProperties(this, valueDeserializer, keyDeserializer, name), null, null);
    }

    protected <K, V> Consumer<K, V> buildUnmanaged(final Deserializer<K> keyDeserializer,
                                                   final Deserializer<V> valueDeserializer,
                                                   final String name) {
        return buildUnmanaged(toProperties(this, null, null, name), keyDeserializer, valueDeserializer);
    }

    protected <K, V> Consumer<K, V> buildUnmanaged(final Properties properties,
                                                   final Deserializer<K> keyDeserializer,
                                                   final Deserializer<V> valueDeserializer) {
        return new KafkaConsumer(properties, keyDeserializer, valueDeserializer);
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
        if(valueDeserializer != null) {
            properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getCanonicalName());
        }
        properties.setProperty(FETCH_MIN_BYTES_CONFIG, String.valueOf(factory.getMinFetchSize().toBytes()));
        properties.setProperty(GROUP_ID_CONFIG, factory.getGroup());
        properties.setProperty(HEARTBEAT_INTERVAL_MS_CONFIG, String.valueOf(factory.getHeartbeatInterval().toMilliseconds()));
        properties.setProperty(MAX_PARTITION_FETCH_BYTES_CONFIG, String.valueOf(factory.getMaxPartitionFetch().toBytes()));
        properties.setProperty(SESSION_TIMEOUT_MS_CONFIG, String.valueOf(factory.getSessionTimeout().toMilliseconds()));

        // TODO: ADD SSL

        properties.setProperty(AUTO_OFFSET_RESET_CONFIG, String.valueOf(factory.getAutoOffsetReset()).toLowerCase());
        properties.setProperty(CONNECTIONS_MAX_IDLE_MS_CONFIG, String.valueOf(factory.getConnectionsMaxIdle().toMilliseconds()));
        properties.setProperty(ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(factory.getAutoCommit()));
        properties.setProperty(RECEIVE_BUFFER_CONFIG, String.valueOf(factory.getReceiveBufferSize().toBytes()));
        properties.setProperty(AUTO_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(factory.getAutoCommitInterval().toMilliseconds()));

        properties.setProperty(CLIENT_ID_CONFIG, factory.buildClientIdProperty(name));
        properties.setProperty(FETCH_MAX_WAIT_MS_CONFIG, String.valueOf(factory.getMaxFetchWaitInterval().toMilliseconds()));
        properties.setProperty(RECONNECT_BACKOFF_MS_CONFIG, String.valueOf(factory.getReconnectBackoffInterval().toMilliseconds()));
        properties.setProperty(RETRY_BACKOFF_MS_CONFIG, String.valueOf(factory.getRetryBackoffInterval().toMilliseconds()));

        // TODO: ADD MORE SSL

        return properties;
    }
}
