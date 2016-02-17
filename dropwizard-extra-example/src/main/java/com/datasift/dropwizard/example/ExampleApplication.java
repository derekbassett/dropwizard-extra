package com.datasift.dropwizard.example;

import com.datasift.dropwizard.example.resources.HelloWorldResource;
import com.datasift.dropwizard.kafka.producer.KafkaProducer;
import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import kafka.serializer.StringEncoder;
import org.glassfish.hk2.api.TypeLiteral;
import org.glassfish.hk2.utilities.binding.AbstractBinder;

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
        environment.jersey().register(new AbstractBinder() {
            @Override
            protected void configure() {
                bind(kafkaProducer).to(new TypeLiteral<KafkaProducer<String, String>>(){});
            }
        });
        environment.jersey().register(HelloWorldResource.class);
    }
}
