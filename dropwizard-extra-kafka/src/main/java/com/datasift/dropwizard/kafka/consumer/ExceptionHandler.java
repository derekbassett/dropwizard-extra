package com.datasift.dropwizard.kafka.consumer;

/**
 * An interface used for handling exceptions
 */
public interface ExceptionHandler {

    void handleException(Throwable exception);

    void handleException(String message, Throwable exception);
}
