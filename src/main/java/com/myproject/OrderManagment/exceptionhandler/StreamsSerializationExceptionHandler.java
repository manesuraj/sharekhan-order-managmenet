package com.myproject.OrderManagment.exceptionhandler;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class StreamsSerializationExceptionHandler implements ProductionExceptionHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamsSerializationExceptionHandler.class);

    @Override
    public ProductionExceptionHandlerResponse handle(ProducerRecord<byte[], byte[]> record, Exception exception) {
        LOGGER.error("Exception in handle : {}  and the record is : {} ", exception.getMessage(), record, exception);
        return ProductionExceptionHandlerResponse.CONTINUE;
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
