package com.datasift.dropwizard.example.resources;

import com.datasift.dropwizard.kafka.consumer.StreamProcessor;
import com.datasift.dropwizard.kafka.producer.KafkaProducer;
import kafka.message.MessageAndMetadata;

import javax.inject.Inject;
import javax.ws.rs.FormParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Simple service that takes a HTTP Resource and sends it to Kafka.
 */
@Path("/helloWorld")
public class HelloWorldResource implements StreamProcessor<String, String> {
    private String topicName = "hello-world";

    private KafkaProducer<String, String> kafkaProducer;

    private BlockingQueue<String> queue = new ArrayBlockingQueue<>(1);

    @Inject
    public void setKafkaProducer(KafkaProducer<String, String> kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    @POST
    public String send(@FormParam("text") String text) {
        kafkaProducer.send(topicName, text);
        // Wait for a queue back
        String reply = "timeout";
        try {
            reply = queue.poll(1000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return reply;
    }

    @Override
    public void process(Iterable<MessageAndMetadata<String, String>> stream, String topic) {
        for(MessageAndMetadata<String, String> messageAndMetadata : stream) {
            queue.add(messageAndMetadata.message());
        }
    }
}
