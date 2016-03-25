package com.datasift.dropwizard.example;

import com.datasift.dropwizard.example.resources.HelloWorldResource;
import com.datasift.dropwizard.kafka8.producer.KafkaProducer;
import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import kafka.serializer.StringDecoder;
import kafka.serializer.StringEncoder;

/**
 * Example application using Dropwizard Kafka and Dropwizard Zookeeper.
 */
public class ExampleApplication extends Application<ExampleApplicationConfiguration> {

    public static void main(String[] args) throws Exception {
        new ExampleApplication().run(args);
    }

    @Override
    public void initialize(Bootstrap<ExampleApplicationConfiguration> bootstrap) {
    }

    @Override
    public void run(ExampleApplicationConfiguration configuration, Environment environment) throws Exception {
        final KafkaProducer<String, String> kafkaProducer = configuration.getKafkaProducerFactory().build(StringEncoder.class, StringEncoder.class, environment, "hello-world");
        final HelloWorldResource helloWorldResource = new HelloWorldResource(kafkaProducer);
        // Create a consumer
        configuration.getKafkaConsumerFactory().processWith(new StringDecoder(null), new StringDecoder(null), helloWorldResource).build(environment, "consumer");
        environment.jersey().register(helloWorldResource);
    }
}
