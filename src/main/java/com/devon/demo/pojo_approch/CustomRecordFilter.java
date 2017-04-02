package com.devon.demo.pojo_approch;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.adapter.RecordFilterStrategy;

/**
 * Created by diwenlao on 3/20/17.
 */
public class CustomRecordFilter implements RecordFilterStrategy<Integer, String> {
    private AtomicLong retryCount = new AtomicLong();
    private   Logger     log        = LoggerFactory.getLogger(CustomRecordFilter.class);

    @Override
    public boolean filter(ConsumerRecord<Integer, String> consumerRecord) {
        log.info("=================filter trigger=============== Thread: {}", Thread.currentThread().getId()+"\\|"+Thread.currentThread().getName());
        if(consumerRecord.value().contains("foo4")){
            log.info("filter foo4");
            return true;
        }else{
            log.info("delegate {}",consumerRecord.value());
            return false;

        }


    }
}
