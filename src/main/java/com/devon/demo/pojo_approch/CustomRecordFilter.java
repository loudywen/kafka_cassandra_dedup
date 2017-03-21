package com.devon.demo.pojo_approch;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.adapter.RecordFilterStrategy;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by diwenlao on 3/20/17.
 */
public class CustomRecordFilter implements RecordFilterStrategy<Integer, String> {
    private AtomicLong retryCount = new AtomicLong();

    @Override
    public boolean filter(ConsumerRecord<Integer, String> consumerRecord) {
        return false;

    }
}
