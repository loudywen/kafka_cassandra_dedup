package com.devon.demo.pojo_approch;

/**
 * Created by Devon on 3/18/2017.
 */

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.text.StrBuilder;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RebalanceListner implements ConsumerRebalanceListener {

  private Logger                                 log            = LoggerFactory
      .getLogger(RebalanceListner.class);
 // private KafkaConsumer consumer;
  private Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap();

/*
  public RebalanceListner(KafkaConsumer con){
    this.consumer=con;
  }
*/

  public void addOffset(String topic, int partition, long offset) {
    currentOffsets
        .put(new TopicPartition(topic, partition), new OffsetAndMetadata(offset, "Commit"));
  }

  public Map<TopicPartition, OffsetAndMetadata> getCurrentOffsets() {
    return currentOffsets;
  }

  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    StrBuilder sb = new StrBuilder();
    for (TopicPartition partition : partitions) {
      sb.append(partition.partition() + " ");
    }
    log.info("Following Partitions Assigned .... "+sb.toString());
  }

  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    StrBuilder sb = new StrBuilder();
    for (TopicPartition partition : partitions) {
      sb.append(partition.partition() + " ");
    }
    log.info("Following Partitions Revoked .... " +sb.toString());

    StrBuilder sb2= new StrBuilder();
    for (TopicPartition tp : currentOffsets.keySet()) {
     sb2.append(tp.partition()+" ");
    }
    System.out.println("Following Partitions committed .... "+sb2.toString());


    currentOffsets.clear();
  }
}
