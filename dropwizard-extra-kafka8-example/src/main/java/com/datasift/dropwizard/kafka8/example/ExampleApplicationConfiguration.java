package com.datasift.dropwizard.kafka8.example;

import com.datasift.dropwizard.kafka8.KafkaConsumerFactory;
import com.datasift.dropwizard.kafka8.KafkaProducerFactory;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

/**
 * An example application configuration using dropwizard kafka.
 */
public class ExampleApplicationConfiguration extends Configuration {

    @NotNull
    @Valid
    private KafkaProducerFactory kafkaProducerFactory = new KafkaProducerFactory();

    @JsonProperty("kafka-producer")
    public KafkaProducerFactory getKafkaProducerFactory() {
        return kafkaProducerFactory;
    }

    @JsonProperty("kafka-producer")
    public void setKafkaProducerFactory(KafkaProducerFactory kafkaProducerFactory) {
        this.kafkaProducerFactory = kafkaProducerFactory;
    }

    @NotNull
    @Valid
    private KafkaConsumerFactory kafkaConsumerFactory = new KafkaConsumerFactory();

    @JsonProperty("kafka-consumer")
    public KafkaConsumerFactory getKafkaConsumerFactory() {
        return kafkaConsumerFactory;
    }

    @JsonProperty("kafka-consumer")
    public void setKafkaConsumerFactory(KafkaConsumerFactory kafkaConsumerFactory) {
        this.kafkaConsumerFactory = kafkaConsumerFactory;
    }
}
