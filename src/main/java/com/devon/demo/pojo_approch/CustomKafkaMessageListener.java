package com.devon.demo.pojo_approch;

import com.devon.demo.KafkaCassandraDedupApplication;
import com.devon.demo.cassandra.DedupRepository;
import com.devon.demo.cassandra.DedupTable;
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
public class CustomKafkaMessageListener implements AcknowledgingMessageListener<Integer, String>,
    ConsumerSeekAware {

  private IKafkaConsumer iKafkaConsumer;
  private Logger log = LoggerFactory.getLogger(CustomKafkaMessageListener.class);
  private DedupRepository dedupRepository;
  public CustomKafkaMessageListener(IKafkaConsumer iKafkaConsumer) {
    this.iKafkaConsumer = iKafkaConsumer;
    this.dedupRepository = (DedupRepository) KafkaCassandraDedupApplication.getApplicationContext().getBean("dedupRepository");
  }


  @Override
  public void onMessage(ConsumerRecord<Integer, String> data, Acknowledgment acknowledgment) {
    String str2 = String
        .format("[Topic: %s] [Key: %s] [Partition: %s] [Offset: %s] [Payload: %s] [Timestamp: %s]",
            data.topic(), data.key(), data.partition(), data.offset(), data.value(),
            Instant.ofEpochMilli(data.timestamp()).atZone(ZoneId.systemDefault())
                .toLocalDateTime());
    iKafkaConsumer.getEvent(str2);
    DedupTable dt = new DedupTable(data.topic(),data.partition(),data.offset()+1);
    dedupRepository.save(dt);
/*
    acknowledgment.acknowledge();
*/
  }

  @Override
  public void registerSeekCallback(ConsumerSeekCallback consumerSeekCallback) {
 /*   List<DedupTable> listDedupTable = dedupRepository.findAll();

    listDedupTable.stream().forEach(dedupTable -> {
      log.info("=======================================Seek on start - topic: {} partition: {} offset: {}", dedupTable.getTopicName(),dedupTable.getPartition(),dedupTable.getOffset());
      consumerSeekCallback.seek(dedupTable.getTopicName(),dedupTable.getPartition(),dedupTable.getOffset());
    });*/

  }

  @Override
  public void onPartitionsAssigned(Map<TopicPartition, Long> map,
      ConsumerSeekCallback consumerSeekCallback) {

    map.forEach((k, v) -> {
      DedupTable dt = dedupRepository.findOffsetByTopicNameAndPartition(k.topic(),k.partition());
      if(dt != null){
        consumerSeekCallback.seek(k.topic(), k.partition(), dt.getOffset());
        log.info("=======================================onPartitionsAssigned - topic: {} partition: {} offset: {}", k.topic(), k.partition(), dt.getOffset());

      }else{
        consumerSeekCallback.seek(k.topic(), k.partition(), v);
        log.info("=======================================db null, get from zookeeper - topic: {} partition: {} offset: {}", k.topic(), k.partition(),v);

      }

    });

  }

  @Override
  public void onIdleContainer(Map<TopicPartition, Long> map,
      ConsumerSeekCallback consumerSeekCallback) {
    //log.info("===================onIdleContainer");

  }
}
