package com.devon.demo.pojo_approch;


import java.lang.reflect.Type;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaNull;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;

/**
 * Created by Devon on 3/31/2017.
 */
public class JsonConverter implements RecordMessageConverter {


  @Override
  public Message<?> toMessage(ConsumerRecord<?, ?> consumerRecord, Acknowledgment acknowledgment,
      Type type) {
    return null;
  }

  @Override
  public ProducerRecord<?, ?> fromMessage(Message<?> message, String defaultTopic) {
    MessageHeaders headers   = message.getHeaders();
    String         topic     = (String)headers.get("kafka_topic", String.class);
    Integer        partition = (Integer)headers.get("kafka_partitionId", Integer.class);
    Object         key       = headers.get("kafka_messageKey");
    Object         payload   = this.convertPayload(message);
    return new ProducerRecord(topic == null?defaultTopic:topic, partition, key, payload);
  }
  protected Object convertPayload(Message<?> message) {
    Object payload = message.getPayload();
    return payload instanceof KafkaNull ?null:payload;
  }
}
