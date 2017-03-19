package com.devon.demo.cassandra;

import java.util.List;
import org.springframework.data.repository.CrudRepository;

/**
 * Created by Devon on 3/19/2017.
 */
public interface DedupRepository extends CrudRepository<DedupTable, Long> {

  DedupTable findOffsetByTopicNameAndPartition(String topicName, int partition);
  List<DedupTable> findAll();
}
