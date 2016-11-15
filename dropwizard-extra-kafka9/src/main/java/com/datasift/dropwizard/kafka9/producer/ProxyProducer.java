package com.datasift.dropwizard.kafka9.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

public class ProxyProducer<K, V> implements Producer<K, V> {

    private final Producer<K, V> producer;

    public ProxyProducer(final Producer<K, V> producer) {
        this.producer = producer;
    }

    @Override
    public void close() {
        producer.close();
    }

    @Override
    public void close(long timeout, java.util.concurrent.TimeUnit unit){
        producer.close(timeout, unit);
    }

    @Override
    public void flush() {
        producer.flush();
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return producer.metrics();
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return producer.partitionsFor(topic);
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
        return producer.send(record);
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
        return producer.send(record, callback);
    }
}
