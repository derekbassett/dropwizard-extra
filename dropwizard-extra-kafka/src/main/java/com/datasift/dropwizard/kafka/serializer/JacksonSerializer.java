package com.datasift.dropwizard.kafka.serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * A Kafka {@link Serializer} for encoding an arbitrary type to a JSON blob.
 */
public class JacksonSerializer<T> implements Serializer<T> {

    private final ObjectMapper mapper;

    public JacksonSerializer(final ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public void close() {
        // Nothing to do
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // Nothing to do
    }

    @Override
    public byte[] serialize(final String topic, final T msg) {
        try {
            return mapper.writeValueAsBytes(msg);
        } catch (final JsonProcessingException ex) {
            throw new RuntimeException(ex);
        }
    }
}
