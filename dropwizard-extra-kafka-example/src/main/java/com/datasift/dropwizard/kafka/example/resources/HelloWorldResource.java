package com.datasift.dropwizard.kafka.example.resources;


import com.datasift.dropwizard.kafka.consumer.ConsumerRecordProcessor;
import com.datasift.dropwizard.kafka.example.api.HelloWorld;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class HelloWorldResource implements ConsumerRecordProcessor<String, HelloWorld> {
    private String topicName = "hello-world";
    private Logger LOG = LoggerFactory.getLogger(HelloWorldResource.class);

    private Producer<String, HelloWorld> kafkaProducer;

    private BlockingQueue<HelloWorld> queue = new ArrayBlockingQueue<>(1);


    public HelloWorldResource(Producer<String, HelloWorld> kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    @POST
    public String send(@FormParam("text") String text) {
        HelloWorld helloWorld = new HelloWorld();
        helloWorld.setText(text);
        kafkaProducer.send(new ProducerRecord<>(topicName, text, helloWorld));
        // Wait for a queue back
        HelloWorld reply = null;
        try {
            reply = queue.poll(1000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return reply != null ? reply.getText() : "timeout";
    }


    @Override
    public void process(ConsumerRecords<String, HelloWorld> records) {
        for(ConsumerRecord<String, HelloWorld> record : records) {
            if (LOG.isInfoEnabled()) {
                LOG.info("offset = {}, key = {}, value = {}", record.offset(), record.key(), record.value());
                queue.add(record.value());
            }
        }
    }
}
