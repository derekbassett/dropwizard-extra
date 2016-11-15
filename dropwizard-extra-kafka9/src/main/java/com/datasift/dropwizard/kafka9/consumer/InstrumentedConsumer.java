package com.datasift.dropwizard.kafka9.consumer;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * A {@link Consumer} that counts the number of messages being received
 */
public class InstrumentedConsumer<K, V> implements Consumer<K, V> {

    private final Consumer<K, V> underlying;
    private final Meter receivedMessages;

    /**
     * Creates a {@link InstrumentedConsumer} to process a stream.
     *
     * @param underlying a {@link Consumer} for consuming messages on.
     */
    public InstrumentedConsumer(final Consumer<K, V> underlying,
                                final MetricRegistry registry,
                                final String name) {
        this.underlying = underlying;
        this.receivedMessages = registry.meter(MetricRegistry.name(name, "received"));
    }

    @Override
    public Set<TopicPartition> assignment() {
        return underlying.assignment();
    }

    @Override
    public Set<String> subscription() {
        return underlying.subscription();
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
        underlying.subscribe(pattern, callback);
    }

    @Override
    public void unsubscribe() {
        underlying.unsubscribe();
    }

    @Override
    public ConsumerRecords poll(long timeout) {
        final ConsumerRecords records = underlying.poll(timeout);
        receivedMessages.mark(records.count());
        return records;
    }

    @Override
    public void commitSync() {
        underlying.commitSync();
    }

    @Override
    public void commitAsync() {
        underlying.commitAsync();
    }

    @Override
    public void commitAsync(OffsetCommitCallback callback) {
        underlying.commitAsync(callback);
    }

    @Override
    public void seek(TopicPartition partition, long offset) {
        underlying.seek(partition, offset);
    }

    @Override
    public void seekToBeginning(TopicPartition... partitions) {
        underlying.seekToBeginning(partitions);
    }

    @Override
    public void seekToEnd(TopicPartition... partitions) {
        underlying.seekToEnd(partitions);
    }

    @Override
    public long position(TopicPartition partition) {
        return underlying.position(partition);
    }

    @Override
    public OffsetAndMetadata committed(TopicPartition partition) {
        return underlying.committed(partition);
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
    public Map<String, List<PartitionInfo>> listTopics() {
        return underlying.listTopics();
    }

    @Override
    public void pause(TopicPartition... partitions) {
        underlying.pause(partitions);
    }

    @Override
    public void resume(TopicPartition... partitions) {
        underlying.resume(partitions);
    }

    @Override
    public void close() {
        underlying.close();
    }

    @Override
    public void wakeup() {
        underlying.wakeup();
    }

    @Override
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        underlying.commitAsync(offsets, callback);
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        underlying.commitSync(offsets);
    }

    @Override
    public void assign(List<TopicPartition> list) {
        underlying.assign(list);
    }

    @Override
    public void subscribe(List<String> topics, ConsumerRebalanceListener callback) {
        underlying.subscribe(topics);
    }

    @Override
    public void subscribe(List<String> topics) {
        underlying.subscribe(topics);
    }

}
