package com.devon.demo.cassandra;

import org.springframework.data.cassandra.mapping.Column;
import org.springframework.data.cassandra.mapping.PrimaryKey;
import org.springframework.data.cassandra.mapping.Table;

/**
 * Created by Devon on 3/19/2017.
 */

@Table(value = "deduptable")
public class DedupTable {


  @PrimaryKey("topic_name")
  private String topicName;

  @Column("partition")
  private int partition;
  @Column("offset")
  private long offset;

  public String getTopicName() {
    return topicName;
  }

  public void setTopicName(String topicName) {
    this.topicName = topicName;
  }

  public int getPartition() {
    return partition;
  }

  public void setPartition(int partition) {
    this.partition = partition;
  }

  public long getOffset() {
    return offset;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  public DedupTable(String topicName, int partition, long offset) {
    this.topicName = topicName;
    this.partition = partition;
    this.offset = offset;
  }


}
