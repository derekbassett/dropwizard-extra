package com.datasift.dropwizard.kafka9.consumer;

import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;

/**
 * All this exception handler does is log messages and shutdown if there is a fatal error.
 */
public class LoggingExceptionHandler<K, V> implements ExceptionHandler {
    private final Logger LOG;
    private final RunnableProcessor<K, V> processor;

    public LoggingExceptionHandler(final Logger LOG, final RunnableProcessor processor) {
        this.LOG = LOG;
        this.processor = processor;
    }

    @Override
    public void handleException(Throwable e) {
        handleException(null, e);
    }

    @Override
    public void handleException(String message, Throwable e) {
        if(e instanceof WakeupException) {
            // Ignore used for shutdown.
        } else if(e instanceof IllegalStateException) {
            error(e);
        } else if(e instanceof Exception) {
            recoverableError((Exception) e);
        } else if(e instanceof Throwable) {
            error(e);
        }
    }

    private void recoverableError(final Exception e) {
        LOG.warn("Recoverable error processing stream ", e);
    }

    private void error(final Throwable e) {
        LOG.error("Unrecoverable error processing stream, shutting down", e);
        processor.requestShutdown();
    }
}
