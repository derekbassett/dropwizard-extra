package com.datasift.dropwizard.kafka9.serializer;

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * A Kafka {@link Deserializer} for decoding an arbitrary type from a JSON blob.
 */
public class JacksonDeserializer<T> implements Deserializer<T> {

    private final Class<T> clazz;
    private final ObjectMapper mapper;

    public JacksonDeserializer(final ObjectMapper mapper, final Class<T> clazz) {
        this.mapper = mapper;
        this.clazz = clazz;
    }

    @Override
    public void close(){
        // Nothing to do
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // Nothing to do
    }

    @Override
    public T deserialize(final String topic, final byte[] bytes) {
        try {
            try {
                return mapper.readValue(bytes, clazz);
            } catch (final JsonParseException ex) {
                final JsonLocation location = ex.getLocation();
                Object src = location.getSourceRef();
                if (src instanceof ByteBuffer) {
                    src = ((ByteBuffer) src).asCharBuffer();
                } else if (src instanceof byte[]) {
                    src = new String((byte[]) src);
                } else if (src instanceof char[]) {
                    src = new String((char[]) src);
                }
                throw new JsonParseException(
                        ex.getMessage(),
                        new JsonLocation(
                                src,
                                location.getByteOffset(),
                                location.getCharOffset(),
                                location.getLineNr(),
                                location.getColumnNr()),
                        ex.getCause());
            }
        } catch (final IOException ex) {
            throw new RuntimeException(ex);
        }
    }
}
