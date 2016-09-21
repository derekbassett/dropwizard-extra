package com.datasift.dropwizard.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SynchronousDeliveryStrategy implements DeliveryStrategy {
    private Logger logger = LoggerFactory.getLogger(SynchronousDeliveryStrategy.class);

    @Override
    public <K, V> void send(final Producer<K, V> producer, final ProducerRecord<K, V> record, final Callback callback) {
        try {
            producer.send(record, callback);
        } catch (KafkaException e) {
            callback.onCompletion(null, e);
            logger.error(e.getMessage(), e);
        }
    }
}
