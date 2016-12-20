package com.datasift.dropwizard.kafka9;

import com.google.common.io.Resources;
import io.dropwizard.configuration.YamlConfigurationFactory;
import io.dropwizard.jackson.Jackson;
import org.junit.Before;
import org.junit.Test;

import javax.validation.Validation;
import javax.validation.Validator;
import java.io.File;
import java.net.InetSocketAddress;

import static org.assertj.core.api.Assertions.assertThat;


/**
 * Tests {@link KafkaConsumerFactory}.
 */
public class KafkaConsumerFactoryIntegrationTest {

    private KafkaConsumerFactory factory = null;

    @Before
    public void setup() throws Exception {

        final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();
        factory = new YamlConfigurationFactory<>(KafkaConsumerFactory.class, validator, Jackson.newObjectMapper(), "dw")
                .build(new File(Resources.getResource("yaml/consumer.yaml").toURI()));
    }

    @Test
    public void testGroup() {
        assertThat(factory.getGroup()).describedAs("group is correctly configured").isEqualTo("test");
    }

    @Test
    public void testBrokers() {
        InetSocketAddress central = InetSocketAddress.createUnresolved("invalid1",9092);
        InetSocketAddress east = InetSocketAddress.createUnresolved("invalid2", 9092);
        assertThat(factory.getBrokers()).describedAs("brokers is correctly configured").contains(central, east);
    }

}
