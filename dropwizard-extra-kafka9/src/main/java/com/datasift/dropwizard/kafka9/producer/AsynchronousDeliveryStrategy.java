package com.datasift.dropwizard.kafka9.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * An asynchronous non-failing strategy.
 */
public class AsynchronousDeliveryStrategy implements DeliveryStrategy {
    private Logger logger = LoggerFactory.getLogger(AsynchronousDeliveryStrategy.class);
    private Executor executor;

    public AsynchronousDeliveryStrategy(Executor executor) {
        this.executor = executor;
    }

    public AsynchronousDeliveryStrategy(int maxThreads) {
        this.executor = Executors.newFixedThreadPool(maxThreads);
    }

    @Override
    public <K, V> void send(final Producer<K, V> producer, final ProducerRecord<K, V> record, final Callback callback) {
        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    producer.send(record, callback);
                } catch (KafkaException e) {
                    callback.onCompletion(null, e);
                    logger.error(e.getMessage(), e);
                }
            }
        });
    }
}
