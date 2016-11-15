package com.datasift.dropwizard.kafka9.producer;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
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

/**
 * A {@link Producer} that is instrumented with metrics.
 */
public class InstrumentedProducer<K, V> implements Producer<K, V> {

    private final Producer<K, V> underlying;
    private final Meter sentMessages;

    public InstrumentedProducer(final Producer<K, V> underlying,
                                final MetricRegistry registry,
                                final String name) {
        this.underlying = underlying;
        this.sentMessages = registry.meter(MetricRegistry.name(name, "sent"));
    }

    @Override
    public void close() {
        underlying.close();
    }

    @Override
    public void close(long timeout, java.util.concurrent.TimeUnit unit){
        underlying.close(timeout, unit);
    }

    @Override
    public void flush() {
        underlying.flush();
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return underlying.metrics();
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return underlying.partitionsFor(topic);
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
        final Future<RecordMetadata> future = underlying.send(record);
        sentMessages.mark();
        return future;
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
        Future<RecordMetadata> future = underlying.send(record, callback);
        sentMessages.mark();
        return future;
    }
}
