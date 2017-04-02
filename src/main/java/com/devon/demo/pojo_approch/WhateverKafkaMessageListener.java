package com.devon.demo.pojo_approch;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.adapter.MessagingMessageListenerAdapter;
import org.springframework.kafka.support.Acknowledgment;

/**
 * Created by Devon on 3/31/2017.
 */
public class WhateverKafkaMessageListener extends MessagingMessageListenerAdapter<Integer,String> implements
    AcknowledgingMessageListener<Integer,String>{
  private Logger log = LoggerFactory.getLogger(WhateverKafkaMessageListener.class);

  public WhateverKafkaMessageListener() {
    super(null, null);
  }


  @Override
  public void onMessage(ConsumerRecord<Integer, String> integerStringConsumerRecord,
      Acknowledgment acknowledgment) {

    log.info(acknowledgment.toString());
  }


}
