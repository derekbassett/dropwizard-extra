package com.datasift.dropwizard.kafka.example;

import com.datasift.dropwizard.kafka.KafkaConsumerFactory;
import com.datasift.dropwizard.kafka.KafkaProducerFactory;
import com.datasift.dropwizard.kafka.PollingProcessorFactory;
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

    @NotNull
    @Valid
    private KafkaConsumerFactory kafkaConsumerFactory = new KafkaConsumerFactory();

    @NotNull
    @Valid
    private PollingProcessorFactory pollingProcessorFactory = new PollingProcessorFactory();

    @JsonProperty("kafka-producer")
    public KafkaProducerFactory getKafkaProducerFactory() {
        return kafkaProducerFactory;
    }

    @JsonProperty("kafka-producer")
    public void setKafkaProducerFactory(KafkaProducerFactory kafkaProducerFactory) {
        this.kafkaProducerFactory = kafkaProducerFactory;
    }

    @JsonProperty("kafka-consumer")
    public KafkaConsumerFactory getKafkaConsumerFactory() {
        return kafkaConsumerFactory;
    }

    @JsonProperty("kafka-consumer")
    public void setKafkaConsumerFactory(KafkaConsumerFactory kafkaConsumerFactory) {
        this.kafkaConsumerFactory = kafkaConsumerFactory;
    }

    @JsonProperty("polling-processor")
    public PollingProcessorFactory getPollingProcessorFactory() { return pollingProcessorFactory; }

    @JsonProperty("polling-processor")
    public void setPollingProcessorFactory(PollingProcessorFactory pollingProcessorFactory) {
        this.pollingProcessorFactory = pollingProcessorFactory;
    }
}
