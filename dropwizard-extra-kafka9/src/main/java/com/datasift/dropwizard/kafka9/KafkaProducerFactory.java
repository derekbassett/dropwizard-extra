package com.datasift.dropwizard.kafka9;

import com.datasift.dropwizard.kafka9.producer.InstrumentedProducer;
import com.datasift.dropwizard.kafka9.producer.ManagedProducer;
import com.datasift.dropwizard.kafka9.producer.ProxyProducer;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.setup.Environment;
import io.dropwizard.util.Duration;
import io.dropwizard.util.Size;
import io.dropwizard.validation.MinDuration;
import io.dropwizard.validation.MinSize;
import io.dropwizard.validation.OneOf;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.Properties;

/**
 * Configuration for the Kafka producer.
 * <p>
 * By default, the producer will be asynchronous
 *
 */
public class KafkaProducerFactory extends KafkaClientFactory {

    /**
     * The acknowledgements to wait for before considering a message as sent.
     */
    public enum Acknowledgement {
        NEVER(0), LEADER(1), ALL(-1);

        private final int value;

        private Acknowledgement(final int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }

    @NotNull
    protected Acknowledgement acknowledgement = Acknowledgement.LEADER;

    @NotNull
    @MinDuration(0)
    protected Duration requestTimeout = Duration.seconds(10);

    @NotNull
    @OneOf({"none","gzip","snappy","lz4"})
    protected String compression = "none";

    @Min(0)
    protected int maxRetries = 3;

    @NotNull
    @MinDuration(0)
    protected Duration retryBackOff = Duration.milliseconds(100);

    @NotNull
    @MinDuration(0)
    protected Duration metadataMaxAge = Duration.milliseconds(300000);

    @NotNull
    @MinDuration(0)
    protected Duration asyncBatchInterval = Duration.milliseconds(0);

    @NotNull
    @MinDuration(0)
    protected Duration maxBlock = Duration.milliseconds(60000);

    @MinSize(1)
    protected Size maxRequestSize = Size.megabytes(1);

    @MinSize(1)
    protected Size batchSize = Size.kilobytes(16);

    @JsonProperty("acknowledgement")
    public Acknowledgement getAcknowledgement() {
        return acknowledgement;
    }

    @JsonProperty("acknowledgement")
    public void setAcknowledgement(final Acknowledgement acknowledgement) {
        this.acknowledgement = acknowledgement;
    }

    @JsonProperty("requestTimeout")
    public Duration getRequestTimeout() {
        return requestTimeout;
    }

    @JsonProperty("requestTimeout")
    public void setRequestTimeout(final Duration requestTimeout) {
        this.requestTimeout = requestTimeout;
    }

    @JsonProperty("compression")
    public String getCompression() {
        return compression;
    }

    @JsonProperty("compression")
    public void setCompression(final String compression) {
        this.compression = compression;
    }

    @JsonProperty("maxRetries")
    public int getMaxRetries() {
        return maxRetries;
    }

    @JsonProperty("maxRetries")
    public void setMaxRetries(final int maxRetries) {
        this.maxRetries = maxRetries;
    }

    @JsonProperty("retryBackOff")
    public Duration getRetryBackOff() {
        return retryBackOff;
    }

    @JsonProperty("retryBackOff")
    public void setRetryBackOff(final Duration retryBackOff) {
        this.retryBackOff = retryBackOff;
    }

    @JsonProperty("metadataMaxAge")
    public Duration getMetadataMaxAge() {
        return metadataMaxAge;
    }

    @JsonProperty("metadataMaxAge")
    public void setMetadataMaxAge(final Duration metadataMaxAge) {
        this.metadataMaxAge = metadataMaxAge;
    }

    @JsonProperty("asyncBatchInterval")
    public Duration getAsyncBatchInterval() {
        return asyncBatchInterval;
    }

    @JsonProperty("asyncBatchInterval")
    public void setAsyncBatchInterval(final Duration asyncBatchInterval) {
        this.asyncBatchInterval = asyncBatchInterval;
    }

    @JsonProperty("maxBlock")
    public Duration getMaxBlock() {
        return maxBlock;
    }

    @JsonProperty("maxBlock")
    public void setMaxBlock(Duration maxBlock) {
        this.maxBlock = maxBlock;
    }

    @JsonProperty("maxRequestSize")
    public Size getMaxRequestSize() {
        return maxRequestSize;
    }

    @JsonProperty("maxRequestSize")
    public void setMaxRequestSize(final Size maxRequestSize) {
        this.maxRequestSize = maxRequestSize;
    }

    @JsonProperty("batchSize")
    public Size getBatchSize() {
        return batchSize;
    }

    @JsonProperty("batchSize")
    public void setBatchSize(final Size batchSize) {
        this.batchSize = batchSize;
    }

    public <V> Producer<byte[], V> build(final Serializer<V> valueSerializer,
                                         final Environment environment,
                                         final String name) {
        return build(new ByteArraySerializer(), valueSerializer, environment, name);
    }

    public <V> Producer<byte[], V> build(final Class<? extends Serializer<V>> valueSerializer,
                                         final Environment environment,
                                         final String name) {
        return build(ByteArraySerializer.class, valueSerializer, environment, name);
    }

    public <K, V> Producer<K, V> build(final Serializer<K> keySerializer,
                                       final Serializer<V> valueSerializer,
                                       final Environment environment,
                                       final String name) {
        return build(keySerializer, valueSerializer, null, environment, name);
    }

    public <K, V> Producer<K, V> build(final Class<? extends Serializer<K>> keySerializer,
                                       final Class<? extends Serializer<V>> valueSerializer,
                                       final Environment environment,
                                       final String name) {
        return build(keySerializer, valueSerializer, null, environment, name);
    }

    public <K, V> Producer<K, V> build(final Serializer<K> keySerializer,
                                       final Serializer<V> valueSerializer,
                                       final Class<? extends Partitioner> partitioner,
                                       final Environment environment,
                                       final String name) {
        final Producer<K, V> producer = build(keySerializer, valueSerializer, partitioner, name);
        environment.lifecycle().manage(new ManagedProducer(producer));
        return new InstrumentedProducer<>(
                producer,
                environment.metrics(),
                name);
    }

    public <K, V> Producer<K, V> build(final Class<? extends Serializer<K>> keySerializer,
                                       final Class<? extends Serializer<V>> valueSerializer,
                                       final Class<? extends Partitioner> partitioner,
                                       final Environment environment,
                                       final String name) {
        final Producer<K, V> producer = build(keySerializer, valueSerializer, partitioner, name);
        environment.lifecycle().manage(new ManagedProducer(producer));
        return new InstrumentedProducer<>(
                producer,
                environment.metrics(),
                name);
    }

    public <K, V> Producer<K, V> build(final Serializer<K> keySerializer,
                                       final Serializer<V> valueSerializer,
                                       final Class<? extends Partitioner> partitioner,
                                       final String name) {
        final Producer<K, V> producer = buildUnmanaged(keySerializer, valueSerializer, partitioner, name);
        return new ProxyProducer<>(producer);
    }

    public <K, V> Producer<K, V> build(final Class<? extends Serializer<K>> keySerializer,
                                       final Class<? extends Serializer<V>> valueSerializer,
                                       final Class<? extends Partitioner> partitioner,
                                       final String name) {
        final Producer<K, V> producer = buildUnmanaged(keySerializer, valueSerializer, partitioner, name);
        return new ProxyProducer<>(producer);
    }

    protected <K, V> Producer<K , V> buildUnmanaged(final Serializer<K> keySerializer,
                                                    final Serializer<V> valueSerializer,
                                                    final Class<? extends Partitioner> partitioner,
                                                    final String name) {
        return buildUnmanaged(toProperties(this, null, null, partitioner, name), keySerializer, valueSerializer);
    }


    protected <K, V> Producer<K , V> buildUnmanaged(final Class<? extends Serializer<K>> keySerializer,
                                                    final Class<? extends Serializer<V>> valueSerializer,
                                                    final Class<? extends Partitioner> partitioner,
                                                    final String name) {
        return buildUnmanaged(toProperties(this, valueSerializer, keySerializer, partitioner, name), null, null);
    }

    protected <K, V> Producer<K , V> buildUnmanaged(final Properties properties,
                                                    final Serializer<K> keySerializer,
                                                    final Serializer<V> valueSerializer) {
        return new KafkaProducer<>(properties, keySerializer, valueSerializer);
    }

    static <K, V> Properties toProperties(final KafkaProducerFactory factory,
                                          final Class<? extends Serializer<V>> valueSerializer,
                                          final Class<? extends Serializer<K>> keySerializer,
                                          final Class<? extends Partitioner> partitioner,
                                          final String name) {
        final Properties properties = new Properties();

        properties.setProperty(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, factory.buildBrokerProperty());
        properties.setProperty(
                ProducerConfig.ACKS_CONFIG, Integer.toString(factory.getAcknowledgement().getValue()));
        properties.setProperty(
                ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, Long.toString(factory.getRequestTimeout().toMilliseconds()));

        if (valueSerializer != null) {
            properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer.getCanonicalName());
        }

        if (keySerializer != null) {
            properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer.getCanonicalName());
        }

        if (partitioner != null) {
            properties.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, partitioner.getCanonicalName());
        }

        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, factory.getCompression());

        properties.setProperty(
                ProducerConfig.RETRIES_CONFIG, Integer.toString(factory.getMaxRetries()));
        properties.setProperty(
                ProducerConfig.RETRY_BACKOFF_MS_CONFIG, Long.toString(factory.getRetryBackOff().toMilliseconds()));
        properties.setProperty(
                ProducerConfig.METADATA_MAX_AGE_CONFIG,
                Long.toString(factory.getMetadataMaxAge().toMilliseconds()));

        properties.setProperty(
                ProducerConfig.LINGER_MS_CONFIG,
                Long.toString(factory.getAsyncBatchInterval().toMilliseconds()));
        properties.setProperty(
                ProducerConfig.MAX_BLOCK_MS_CONFIG,
                Long.toString(factory.getMaxBlock().toMilliseconds()));

        properties.setProperty(
                ProducerConfig.MAX_REQUEST_SIZE_CONFIG, Long.toString(factory.getMaxRequestSize().toBytes()));

        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Long.toString(factory.getBatchSize().toBytes()));

        properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, factory.buildClientIdProperty(name));

        return properties;
    }
}
