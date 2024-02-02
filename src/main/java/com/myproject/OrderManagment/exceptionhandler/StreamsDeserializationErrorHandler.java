package com.myproject.OrderManagment.exceptionhandler;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class StreamsDeserializationErrorHandler implements DeserializationExceptionHandler {
    int errorCounter = 0;

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamsDeserializationErrorHandler.class);

    @Override
    public DeserializationHandlerResponse handle(ProcessorContext context, ConsumerRecord<byte[], byte[]> record, Exception exception) {

        LOGGER.error("Exception is : {} and the Kafka Record is : {} " , exception.getMessage(), record,  exception);
        LOGGER.error("errorCounter is : {} " , errorCounter);
        if(errorCounter < 10){
            errorCounter++;
            return DeserializationHandlerResponse.CONTINUE;
        }
        return DeserializationHandlerResponse.FAIL;
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
