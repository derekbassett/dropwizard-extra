package com.datasift.dropwizard.kafka;

import com.google.common.io.Resources;
import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.jackson.Jackson;
import org.junit.Before;
import org.junit.Test;

import javax.validation.Validation;
import javax.validation.Validator;
import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;


/**
 * Tests {@link KafkaConsumerFactory}.
 */
public class KafkaConsumerFactoryIntegrationTest {

    private KafkaConsumerFactory factory = null;

    @Before
    public void setup() throws Exception {

        final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();
        factory = new ConfigurationFactory<>(KafkaConsumerFactory.class, validator, Jackson.newObjectMapper(), "dw")
                .build(new File(Resources.getResource("yaml/consumer.yaml").toURI()));
    }

    @Test
    public void testGroup() {
        assertThat(factory.getGroup()).describedAs("group is correctly configured").isEqualTo("test");
    }

}
