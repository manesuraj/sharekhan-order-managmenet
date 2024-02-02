package com.myproject.OrderManagment.util;


import com.myproject.OrderManagment.domain.Order;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneOffset;

public class OrderTimeStampExtractor implements TimestampExtractor {

    private static final Logger LOGGER = LoggerFactory.getLogger(OrderTimeStampExtractor.class);

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        var orderRecord = (Order) record.value();
        if(orderRecord!=null && orderRecord.orderedDateTime()!=null){
            var timeStamp = orderRecord.orderedDateTime();
            LOGGER.info("TimeStamp in extractor : {} ", timeStamp);
            var instant = timeStamp.toInstant(ZoneOffset.ofHours(-6)).toEpochMilli();
            LOGGER.info("instant in extractor : {} ", instant);
            return instant;
        }
        //fallback to stream time
        return partitionTime;
    }
}
