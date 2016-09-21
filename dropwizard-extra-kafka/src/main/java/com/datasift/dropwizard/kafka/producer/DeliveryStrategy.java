package com.datasift.dropwizard.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * How do we handle delivery of the message.
 */
public interface DeliveryStrategy {

    /**
     * Sends a message to a kafka producer and somehow deals with failures.
     *
     * @param producer the backing kafka producer
     * @param record the prepared kafka message (ready to ship)
     * @param callback a callback that handles messages that could not be delivered with best-effort.
     * @param <K> the key type of the message.
     * @param <V> the value type of the message.
     */
    <K, V> void send(Producer<K, V> producer, ProducerRecord<K, V> record, Callback callback);

}
