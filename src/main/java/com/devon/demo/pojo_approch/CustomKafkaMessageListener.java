package com.devon.demo.pojo_approch;

import com.devon.demo.KafkaCassandraDedupApplication;
import com.devon.demo.cassandra.DedupRepository;
import com.devon.demo.cassandra.DedupTable;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.listener.ErrorHandler;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryListener;


/**
 * Created by Devon on 3/18/2017.
 */
public class CustomKafkaMessageListener implements AcknowledgingMessageListener<Integer, String>,
    ConsumerSeekAware, ErrorHandler, RetryListener {

  private IKafkaConsumer iKafkaConsumer;
  private Logger log = LoggerFactory.getLogger(CustomKafkaMessageListener.class);
  private       DedupRepository  dedupRepository;
  private final SimpleDateFormat simpleDateFormat;
  private AtomicLong retryCount = new AtomicLong();

  private ThreadLocal<ConsumerSeekCallback> threadLocal = new ThreadLocal<>();

  public CustomKafkaMessageListener(IKafkaConsumer iKafkaConsumer) {
    this.iKafkaConsumer = iKafkaConsumer;
    this.dedupRepository = (DedupRepository) KafkaCassandraDedupApplication.getApplicationContext()
        .getBean("dedupRepository");
    simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
  }


  @Override
  public void onMessage(ConsumerRecord<Integer, String> data, Acknowledgment acknowledgment) {
    String str2 = String
        .format("[Topic: %s] [Key: %s] [Partition: %s] [Offset: %s] [Payload: %s] [Timestamp: %s]",
            data.topic(), data.key(), data.partition(), data.offset(), data.value(),
            Instant.ofEpochMilli(data.timestamp()).atZone(ZoneId.systemDefault())
                .toLocalDateTime());
    DedupTable dt = new DedupTable(data.topic(), data.partition(), data.offset() + 1);
    iKafkaConsumer.getEvent(str2);
    dedupRepository.save(dt);
    acknowledgment.acknowledge();



/*    try {

      iKafkaConsumer.getEvent(str2);
      // log.info("Saving offset");
      //dedupRepository.save(dt);
      acknowledgment.acknowledge();

    } catch (Exception e) {
      log.error("exception");
      /*//* DedupTable dt = new DedupTable(data.topic(), data.partition(), data.offset() );
      //dedupRepository.save(dt);*//**//*

    }*/

  }

  @Override
  public void registerSeekCallback(ConsumerSeekCallback consumerSeekCallback) {
    log.info("===================Thread: {}", Thread.currentThread().getId());
    threadLocal.set(consumerSeekCallback);
  }

  @Override
  public void onPartitionsAssigned(Map<TopicPartition, Long> map,
      ConsumerSeekCallback consumerSeekCallback) {

    map.forEach((k, v) -> {
      DedupTable dt = dedupRepository.findOffsetByTopicNameAndPartition(k.topic(), k.partition());
      if (dt != null) {

        if (v <= dt.getOffset()) {
          consumerSeekCallback.seek(k.topic(), k.partition(), dt.getOffset());
          log.info(
              "Thread {} onPartitionsAssigned - topic: {} partition: {} offset: {}",
              Thread.currentThread().getId() + "\\|" + Thread.currentThread().getName(), k.topic(),
              k.partition(), dt.getOffset());
        } else {
          consumerSeekCallback.seek(k.topic(), k.partition(), v);
          log.info(
              "Thread {} DB less then zookeeper, get from zookeeper - topic: {} partition: {} offset: {}",
              Thread.currentThread().getId() + "\\|" + Thread.currentThread().getName(), k.topic(),
              k.partition(), v);
        }

      }

    });

  }

  @Override
  public void onIdleContainer(Map<TopicPartition, Long> map,
      ConsumerSeekCallback consumerSeekCallback) {
    //log.info("===================onIdleContainer");

  }


  @Override
  public void handle(Exception thrownException, ConsumerRecord<?, ?> data) {
    log.error(
        "==========error handler =========== topic: {}, partition: {}, offset: {} payload: {}",
        data.topic(), data.partition(),
        data.offset(), data.value());
    log.error(thrownException.getMessage(), thrownException);
    log.error("==========error handler re seek the fail offset =========== ");
    //threadLocal.get().seek(data.topic(), data.partition(), data.offset());
  }

  @Override
  public <CustomKafkaMessageListener, Exception extends Throwable> boolean open(
      RetryContext retryContext,
      RetryCallback<CustomKafkaMessageListener, Exception> retryCallback) {

    //log.info("========open==========");
    return true;
  }

  @Override
  public <CustomKafkaMessageListener, Exception extends Throwable> void close(
      RetryContext retryContext,
      RetryCallback<CustomKafkaMessageListener, Exception> retryCallback, Throwable throwable) {
   // log.info("=========close=========");

  }

  @Override
  public <CustomKafkaMessageListener, Exception extends Throwable> void onError(
      RetryContext retryContext,
      RetryCallback<CustomKafkaMessageListener, Exception> retryCallback, Throwable throwable) {

   log.info("========onError========== count: {}", retryContext.getRetryCount());

  }
}
