package com.datasift.dropwizard.kafka9;

import com.google.common.io.Resources;
import io.dropwizard.configuration.YamlConfigurationFactory;
import io.dropwizard.jackson.Jackson;
import org.junit.Before;
import org.junit.Test;

import javax.validation.Validation;
import javax.validation.Validator;
import java.io.File;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertThat;

/**
 * Tests {@link PollingProcessorFactory}.
 */
public class PollingProcessorFactoryIntegrationTest {

    private PollingProcessorFactory factory = null;

    @Before
    public void setup() throws Exception {

        final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();
        factory = new YamlConfigurationFactory<>(PollingProcessorFactory.class, validator, Jackson.newObjectMapper(), "dw")
                .build(new File(Resources.getResource("yaml/polling.yaml").toURI()));
    }

    @Test
    public void testTopics() {
        assertThat("has correct partition configuration",
                   factory.getTopics(),
                   allOf(hasItem("foo"), hasItem("bar")));
    }

}
