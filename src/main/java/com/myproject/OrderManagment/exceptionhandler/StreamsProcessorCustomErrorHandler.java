package com.myproject.OrderManagment.exceptionhandler;

import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamsProcessorCustomErrorHandler implements StreamsUncaughtExceptionHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamsProcessorCustomErrorHandler.class);

    @Override
    public StreamThreadExceptionResponse handle(Throwable exception) {
        LOGGER.error("Exception in the Application : {} ",exception.getMessage(), exception);
        if(exception instanceof StreamsException){
            var cause = exception.getCause();
            if(cause.getMessage().equals("Transient Error")){
                //return StreamThreadExceptionResponse.REPLACE_THREAD;
                return StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            }
        }
        LOGGER.error("Shutdown the client");
        //return StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        return StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
    }
}
