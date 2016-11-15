package com.datasift.dropwizard.kafka9.example.api;

import org.hibernate.validator.constraints.NotBlank;

/**
 * This is a simple Json object passed around in Kafka.
 */
public class HelloWorld {

    @NotBlank
    private String text;

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

}
