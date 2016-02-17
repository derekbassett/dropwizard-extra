package com.datasift.dropwizard.example.resources;

import com.datasift.dropwizard.kafka.producer.KafkaProducer;

import javax.inject.Inject;
import javax.ws.rs.FormParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * Simple service that takes a HTTP Resource and sends it to Kafka.
 */
@Path("/helloWorld")
public class HelloWorldResource {
    private String topicName = "hello-world";

    private KafkaProducer<String, String> kafkaProducer;

    @Inject
    public void setKafkaProducer(KafkaProducer<String, String> kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    @POST
    public void send(@FormParam("text") String text) {
        kafkaProducer.send(topicName, text);
    }

}
