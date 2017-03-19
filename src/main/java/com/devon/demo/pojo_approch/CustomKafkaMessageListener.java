package com.devon.demo.pojo_approch;

import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.support.Acknowledgment;


/**
 * Created by Devon on 3/18/2017.
 */
public class CustomKafkaMessageListener implements AcknowledgingMessageListener<Integer, String> ,ConsumerSeekAware {

  private IKafkaConsumer iKafkaConsumer;
  private Logger log = LoggerFactory.getLogger(CustomKafkaMessageListener.class);
  public CustomKafkaMessageListener(IKafkaConsumer iKafkaConsumer) {
    this.iKafkaConsumer = iKafkaConsumer;
  }


  @Override
  public void onMessage(ConsumerRecord<Integer, String> data, Acknowledgment acknowledgment) {
    String str2 = String
        .format("[Topic: %s] [Key: %s] [Partition: %s] [Offset: %s] [Payload: %s] [Timestamp: %s]",
            data.topic(), data.key(), data.partition(), data.offset(), data.value(),
            Instant.ofEpochMilli(data.timestamp()).atZone(ZoneId.systemDefault())
                .toLocalDateTime());
    iKafkaConsumer.getEvent(str2);
   acknowledgment.acknowledge();
  }

  @Override
  public void registerSeekCallback(ConsumerSeekCallback consumerSeekCallback) {
    log.info("===================registerSeekCallback");

  }

  @Override
  public void onPartitionsAssigned(Map<TopicPartition, Long> map,
      ConsumerSeekCallback consumerSeekCallback) {
    log.info("===================onPartitionsAssigned");
  }

  @Override
  public void onIdleContainer(Map<TopicPartition, Long> map,
      ConsumerSeekCallback consumerSeekCallback) {
    log.info("===================onIdleContainer");

  }
}
