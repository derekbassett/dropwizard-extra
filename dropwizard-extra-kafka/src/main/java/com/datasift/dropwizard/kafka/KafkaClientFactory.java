package com.datasift.dropwizard.kafka;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import io.dropwizard.util.Duration;
import org.hibernate.validator.constraints.NotEmpty;
import org.hibernate.validator.valuehandling.UnwrapValidatedValue;

import javax.validation.constraints.NotNull;
import java.net.InetSocketAddress;

/**
 * Base configuration for Kafka clients.
 *
 */
abstract public class KafkaClientFactory {

    static final int DEFAULT_BROKER_PORT = 9092;

    @NotNull
    protected Duration socketTimeout = Duration.seconds(30);

    @NotEmpty
    protected ImmutableSet<InetSocketAddress> brokers = ImmutableSet.of();
    @UnwrapValidatedValue
    protected Optional<String> clientIdSuffix = Optional.absent();

    /**
     * Returns the time to wait on a network socket before timing out requests.
     *
     * @return the time to wait on a network socket before timing out requests.
     */
    @JsonProperty
    public Duration getSocketTimeout() {
        return socketTimeout;
    }

    /**
     * Sets the time to wait on a network socket before timing out requests.
     *
     * @param socketTimeout the time to wait on a network socket before timing out requests.
     */
    @JsonProperty
    public void setSocketTimeout(final Duration socketTimeout) {
        this.socketTimeout = socketTimeout;
    }

    @JsonProperty("brokers")
    public ImmutableSet<InetSocketAddress> getBrokers() {
        return brokers;
    }

    @JsonProperty("brokers")
    public void setBrokers(final ImmutableSet<InetSocketAddress> brokers) {
        this.brokers = brokers;
    }

    @JsonProperty("clientIdSuffix")
    public Optional<String> getClientIdSuffix() {
        return clientIdSuffix;
    }

    @JsonProperty("clientIdSuffix")
    public void setClientIdSuffix(final Optional<String> clientIdSuffix) {
        this.clientIdSuffix = clientIdSuffix;
    }

    /**
     * Builds the broker property based on the currently configured brokers
     */
    public String buildBrokerProperty() {
        final StringBuilder sb = new StringBuilder(10 * getBrokers().size());
        for (final InetSocketAddress addr : getBrokers()) {
            final int port = addr.getPort() == 0 ? DEFAULT_BROKER_PORT : addr.getPort();
            sb.append(addr.getHostString()).append(':').append(port).append(',');
        }
        return sb.substring(0, sb.length() - 1);
    }

    /**
     * Builds the client id property
     * @param name
     * @return
     */
    public String buildClientIdProperty(final String name) {
        final StringBuilder clientId = new StringBuilder(name);
        if (getClientIdSuffix().isPresent()) {
            clientId.append('-').append(getClientIdSuffix().get());
        }
        return clientId.toString();
    }

}
