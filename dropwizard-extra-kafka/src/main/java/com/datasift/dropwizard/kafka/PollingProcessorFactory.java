package com.datasift.dropwizard.kafka;

import com.datasift.dropwizard.kafka.consumer.ConsumerRecordProcessor;
import com.datasift.dropwizard.kafka.consumer.KafkaConsumerHealthCheck;
import com.datasift.dropwizard.kafka.consumer.PollingProcessor;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.dropwizard.setup.Environment;
import io.dropwizard.util.Duration;
import io.dropwizard.validation.MinDuration;
import org.apache.kafka.clients.consumer.Consumer;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * A Factory for building a configured {@link PollingProcessor}.
 */
public class PollingProcessorFactory {

    // This defaults to basically waiting for Godot (Forever) with reading the consumer you can of course tweak as you want.
    @MinDuration(0)
    protected Duration pollTimeout = Duration.milliseconds(Long.MAX_VALUE);

    protected boolean autoCommit = true;

    @Min(-1)
    protected int batchCount = 0;

    @NotNull
    @NotEmpty
    protected ImmutableList<String> topics = ImmutableList.of();

    @NotNull
    protected boolean shutdownOnFatal = false;

    @MinDuration(0)
    protected Duration shutdownGracePeriod = Duration.seconds(5);

    @MinDuration(0)
    protected Duration startDelay = Duration.seconds(2);

    private static final String DEFAULT_NAME = "polling-processor-default";

    /**
     * Setup with a polling timeout
     * @param pollTimeout
     * @return
     */
    @JsonProperty
    public void setPollTimeout(final Duration pollTimeout) {
        this.pollTimeout = pollTimeout;
    }

    @JsonProperty
    public Duration getPollTimeout() {
        return pollTimeout;
    }

    @JsonProperty
    public boolean isAutoCommit() {
        return autoCommit;
    }

    @JsonProperty
    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    @JsonProperty
    public int getBatchCount() {
        return batchCount;
    }

    @JsonProperty
    public void setBatchCount(int batchCount) {
        this.batchCount = batchCount;
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
    public ImmutableList<String> getTopics() {
        return topics;
    }

    @JsonProperty
    public void setTopics(ImmutableList<String> topics) {
        this.topics = topics;
    }

    @JsonProperty
    public Duration getStartDelay() {
        return startDelay;
    }

    @JsonProperty
    public void setStartDelay(final Duration startDelay) {
        this.startDelay = startDelay;
    }

    /**
     * Builds a {@link PollingProcessor} instance for the given {@link Environment}.
     *
     * @param environment the {@link Environment} to build {@link PollingProcessor} instances for.
     *
     * @return a managed and configured {@link PollingProcessor}.
     */
    public <K,V> PollingProcessor<K, V> build(final Environment environment, final Consumer<K, V> consumer, final ConsumerRecordProcessor<K, V> processor) {
        return build(environment, consumer, processor, DEFAULT_NAME);
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
    public <K,V> PollingProcessor<K, V> build(final Environment environment, final Consumer<K, V> consumer, final ConsumerRecordProcessor<K, V> processor, final String name) {

        final ScheduledExecutorService executor = environment.lifecycle()
                .scheduledExecutorService(name)
                    .threads(1)
                    .shutdownTime(shutdownGracePeriod)
                    .build();

        return build(environment, consumer, processor, executor, name);
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
    public <K,V> PollingProcessor<K, V> build(final Environment environment, final Consumer<K, V> consumer, final ConsumerRecordProcessor<K, V> processor, final ScheduledExecutorService executor, final String name) {
        final PollingProcessor<K, V> pollingProcessor = build(consumer, processor, executor);

        // manage the consumer
        environment.lifecycle().manage(pollingProcessor);
        environment.lifecycle().addServerLifecycleListener(pollingProcessor);

        // add health checks
        environment.healthChecks().register(name, new KafkaConsumerHealthCheck(pollingProcessor));

        return pollingProcessor;
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
    private <K, V> PollingProcessor<K, V> build(final Consumer<K, V> consumer, final ConsumerRecordProcessor<K, V> processor, final ScheduledExecutorService executor) {
        final PollingProcessor<K, V> poller = new PollingProcessor<>(
                consumer,
                topics,
                processor,
                executor,
                pollTimeout,
                shutdownOnFatal,
                startDelay,
                autoCommit,
                batchCount
        );
        return poller;
    }
}
