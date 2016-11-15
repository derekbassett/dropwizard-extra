package com.datasift.dropwizard.kafka9.example;

import com.datasift.dropwizard.kafka9.example.api.HelloWorld;
import com.datasift.dropwizard.kafka9.example.resources.HelloWorldResource;
import com.datasift.dropwizard.kafka9.serializer.JacksonDeserializer;
import com.datasift.dropwizard.kafka9.serializer.JacksonSerializer;
import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

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
        JacksonSerializer<HelloWorld> valueSerializer = new JacksonSerializer(environment.getObjectMapper());

        final Producer<String, HelloWorld> kafkaProducer = configuration.getKafkaProducerFactory().build(new StringSerializer(), valueSerializer, environment, "producer");
        final HelloWorldResource helloWorldResource = new HelloWorldResource(kafkaProducer);

        JacksonDeserializer<HelloWorld> valueDeserializer = new JacksonDeserializer<>(environment.getObjectMapper(), HelloWorld.class);

        // Create a consumer
        // helloWorldResource
        final Consumer<String, HelloWorld> consumer = configuration.getKafkaConsumerFactory().build(environment, valueDeserializer, new StringDeserializer(), "consumer");
        configuration.getPollingProcessorFactory().build(environment, consumer, helloWorldResource, "polling");
        environment.jersey().register(helloWorldResource);
    }
}
