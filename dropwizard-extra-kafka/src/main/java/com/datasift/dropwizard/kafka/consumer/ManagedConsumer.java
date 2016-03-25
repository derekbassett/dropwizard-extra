package com.datasift.dropwizard.kafka.consumer;

import io.dropwizard.lifecycle.Managed;
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
 * A {@link Consumer} that manages using dropwizard to closes automatically when stopped.
 */
public class ManagedConsumer<K, V> implements Consumer<K, V>, Managed {
    private final Consumer<K, V> consumer;

    /**
     * Creates a {@link ManagedConsumer} to process a stream.
     *
     * @param consumer a {@link Consumer} for consuming messages on.
     */
    public ManagedConsumer(final Consumer<K, V> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void start() throws Exception {
    }

    /**
     * Stops this {@link ManagedConsumer} immediately.
     *
     * @throws Exception if an error occurs on stop
     */
    @Override
    public void stop() throws Exception {
        consumer.close();
    }

    @Override
    public Set<TopicPartition> assignment() {
        return consumer.assignment();
    }

    @Override
    public Set<String> subscription() {
        return consumer.subscription();
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
        consumer.subscribe(pattern, callback);
    }

    @Override
    public void unsubscribe() {
        consumer.unsubscribe();
    }

    @Override
    public ConsumerRecords poll(long timeout) {
        return consumer.poll(timeout);
    }

    @Override
    public void commitSync() {
        consumer.commitSync();
    }

    @Override
    public void commitAsync() {
        consumer.commitAsync();
    }

    @Override
    public void commitAsync(OffsetCommitCallback callback) {
        consumer.commitAsync(callback);
    }

    @Override
    public void seek(TopicPartition partition, long offset) {
        consumer.seek(partition, offset);
    }

    @Override
    public void seekToBeginning(TopicPartition... partitions) {
        consumer.seekToBeginning(partitions);
    }

    @Override
    public void seekToEnd(TopicPartition... partitions) {
        consumer.seekToEnd(partitions);
    }

    @Override
    public long position(TopicPartition partition) {
        return consumer.position(partition);
    }

    @Override
    public OffsetAndMetadata committed(TopicPartition partition) {
        return consumer.committed(partition);
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return consumer.metrics();
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return consumer.partitionsFor(topic);
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        return consumer.listTopics();
    }

    @Override
    public void pause(TopicPartition... partitions) {
        consumer.pause(partitions);
    }

    @Override
    public void resume(TopicPartition... partitions) {
        consumer.resume(partitions);
    }

    @Override
    public void close() {
        consumer.close();
    }

    @Override
    public void wakeup() {
        consumer.wakeup();
    }

    @Override
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        consumer.commitAsync(offsets, callback);
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        consumer.commitSync(offsets);
    }

    @Override
    public void assign(List<TopicPartition> list) {
        consumer.assign(list);
    }

    @Override
    public void subscribe(List<String> topics, ConsumerRebalanceListener callback) {
        consumer.subscribe(topics);
    }

    @Override
    public void subscribe(List<String> topics) {
        consumer.subscribe(topics);
    }

}
