package com.devon.demo;

import com.devon.demo.cassandra.DedupRepository;
import com.devon.demo.cassandra.DedupTable;
import com.devon.demo.pojo_approch.IKafkaConsumer;
import com.devon.demo.pojo_approch.KafkaPOJOConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.support.TopicPartitionInitialOffset;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@SpringBootApplication
@EnableCassandraRepositories(basePackages = {"com.devon.demo.cassandra"})
@RestController
public class KafkaCassandraDedupApplication implements IKafkaConsumer, ApplicationContextAware {


  private static ApplicationContext applicationContext;
  private Logger                         log   = LoggerFactory
      .getLogger(KafkaCassandraDedupApplication.class);
  static  KafkaCassandraDedupApplication k     = new KafkaCassandraDedupApplication();
  private AtomicLong                     count = new AtomicLong();
  static ConcurrentMessageListenerContainer<Integer, String> container2;

  public static void main(String[] args) throws InterruptedException {
    SpringApplication.run(KafkaCassandraDedupApplication.class, args);

/*    DedupRepository dedupRepository = (DedupRepository) KafkaCassandraDedupApplication
        .getApplicationContext().getBean("dedupRepository");

    List<DedupTable> listDedupTable = dedupRepository.findAll();

    List<TopicPartitionInitialOffset> topic1PartitionS = new ArrayList<>();

    for (int x = 0; x < listDedupTable.size(); x++) {
      topic1PartitionS.add(new TopicPartitionInitialOffset(listDedupTable.get(x).getTopicName(),
          listDedupTable.get(x).getPartition(), listDedupTable.get(x).getOffset()));
    }

    KafkaPOJOConfig kconfig = new KafkaPOJOConfig();

    ConcurrentKafkaListenerContainerFactory<Integer, String> factory = kconfig
        .factory(k, topic1PartitionS);
    factory.setAutoStartup(true);
    factory.getContainerProperties()
        .setAckMode(AbstractMessageListenerContainer.AckMode.MANUAL_IMMEDIATE);
    factory.getContainerProperties().setAckOnError(false);
    factory.setRetryTemplate(new RetryTemplate());
    CustomKafkaMessageListener ckml = new CustomKafkaMessageListener(k);
    factory.getContainerProperties().setErrorHandler(ckml);*/

  /*  factory.createListenerContainer(new KafkaListenerEndpoint() {
      @Override
      public String getId() {
        return "diwen";
      }

      @Override
      public String getGroup() {
        return "pojokafka";
      }

      @Override
      public Collection<String> getTopics() {
        return Arrays.asList("dedup");
      }

      @Override
      public Collection<TopicPartitionInitialOffset> getTopicPartitions() {
        return topic1PartitionS;
      }

      @Override
      public Pattern getTopicPattern() {
        return null;
      }

      @Override
      public void setupListenerContainer(MessageListenerContainer listenerContainer,
          MessageConverter messageConverter) {
        listenerContainer.setupMessageListener(ckml);
      }
    });

*/

    DedupRepository dedupRepository = (DedupRepository) KafkaCassandraDedupApplication
        .getApplicationContext().getBean("dedupRepository");

    List<DedupTable> listDedupTable = dedupRepository.findAll();

    KafkaPOJOConfig kconfig = new KafkaPOJOConfig();

    TopicPartitionInitialOffset[] topic1PartitionS = new TopicPartitionInitialOffset[listDedupTable
        .size()];

    for (int x = 0; x < listDedupTable.size(); x++) {
      topic1PartitionS[x] = new TopicPartitionInitialOffset(listDedupTable.get(x).getTopicName(),
          listDedupTable.get(x).getPartition(), listDedupTable.get(x).getOffset());
    }

    ContainerProperties containerProps = new ContainerProperties("dedup");

    ConcurrentMessageListenerContainer<Integer, String> container = kconfig
        .createContainer(containerProps, k);
    container.setBeanName("dedup-");

    container.start();

//    ContainerProperties containerProps2 = new ContainerProperties("dedup2");

//    container2 = kconfig
//        .createContainer(containerProps2, k);
//    container2.setBeanName("dedup2-");
//    container2.start();
    //kconfig.factory(containerProps,k);

  }


  @GetMapping("/stop")
  public void stop() {
    container2.stop();
  }

  @GetMapping("/start")
  public void start() {
    container2.start();
  }

  @Override
  public void getEvent(String str) {
    log.info("{} - Thread: {}", str,
        Thread.currentThread().getId() + "\\|" + Thread.currentThread().getName());
    throw new RuntimeException();

  }

  @Override
  public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
    this.applicationContext = applicationContext;
  }

  public static ApplicationContext getApplicationContext() {
    return applicationContext;
  }


}
