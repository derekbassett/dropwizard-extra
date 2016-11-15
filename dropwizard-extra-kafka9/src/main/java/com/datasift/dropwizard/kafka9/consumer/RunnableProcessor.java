package com.datasift.dropwizard.kafka9.consumer;

/**
 * The interface for all runnable processors adds some controls for health check and logging.
 */
public interface RunnableProcessor<K, V> {

    /**
     * Is Running
     * @return
     */
    boolean isRunning();

    /**
     * Request the Runnable Process to shutdown.
     */
    void requestShutdown();
}
