package com.datasift.dropwizard.kafka9.consumer;

import com.codahale.metrics.health.HealthCheck;

/**
 * A {@link HealthCheck} to monitor the health of a {@link RunnableProcessor}.
 */
public class KafkaConsumerHealthCheck extends HealthCheck {

    private final RunnableProcessor consumer;

    /**
     * Create a new {@link HealthCheck} instance with the given name.
     *
     * @param consumer the {@link RunnableProcessor} to monitor the health of.
     */
    public KafkaConsumerHealthCheck(final RunnableProcessor consumer) {
        this.consumer = consumer;
    }

    /**
     * Checks that the {@link RunnableProcessor} is still in its <i>running</i> state.
     *
     * @return true if the {@link RunnableProcessor} is still running properly; false if it is not.
     *
     * @throws Exception if there is an error checking the state of the {@link RunnableProcessor}.
     */
    @Override
    protected Result check() throws Exception {
        return consumer.isRunning()
                ? Result.healthy()
                : Result.unhealthy("Consumer not consuming any partitions");
    }
}
