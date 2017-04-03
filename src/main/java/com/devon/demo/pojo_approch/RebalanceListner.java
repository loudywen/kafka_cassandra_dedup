package com.devon.demo.pojo_approch;

/**
 * Created by Devon on 3/18/2017.
 */

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.text.StrBuilder;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RebalanceListner implements ConsumerRebalanceListener {

  private Logger                                 log            = LoggerFactory
      .getLogger(RebalanceListner.class);
  private Consumer consumer;
  private Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap();


  public RebalanceListner(Consumer con){
    this.consumer=con;
  }


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
    System.out.println("Following Partitions Revoked ....");
    for(TopicPartition partition: partitions)
      System.out.println(partition.partition()+",");


    System.out.println("Following Partitions commited ...." );
    for(TopicPartition tp: currentOffsets.keySet())
      System.out.println(tp.partition());

    consumer.commitSync(currentOffsets);
    currentOffsets.clear();
  }
}
