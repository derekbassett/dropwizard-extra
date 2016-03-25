package com.datasift.dropwizard.kafka.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests {@link JacksonSerializer}
 */
public class JacksonSerializerTest {

    private ObjectMapper mapper;
    private JacksonSerializer serializer;

    @Before
    public void setUp() throws Exception {

        mapper = Mockito.mock(ObjectMapper.class);
        serializer = new JacksonSerializer(mapper);

    }

    @Test
    public void serialize() throws Exception {
        final String topic = "topic";
        final Object msg = "abc";
        serializer.serialize(topic, msg);

        Mockito.verify(mapper).writeValueAsBytes(msg);
    }
}