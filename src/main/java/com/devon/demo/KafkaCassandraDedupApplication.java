package com.devon.demo;

import com.devon.demo.pojo_approch.IKafkaConsumer;
import com.devon.demo.pojo_approch.KafkaPOJOConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.config.ContainerProperties;

@SpringBootApplication
public class KafkaCassandraDedupApplication implements IKafkaConsumer {

  private Logger                         log = LoggerFactory.getLogger(KafkaCassandraDedupApplication.class);
  static  KafkaCassandraDedupApplication k   = new KafkaCassandraDedupApplication();

  public static void main(String[] args) throws InterruptedException {
    SpringApplication.run(KafkaCassandraDedupApplication.class, args);

    KafkaPOJOConfig kconfig = new KafkaPOJOConfig();

    ContainerProperties containerProps = new ContainerProperties(
        "dedup");
    ConcurrentMessageListenerContainer<Integer, String> container = kconfig
        .createContainer(containerProps, k);
    container.start();
   /* Thread.sleep(5000);
    container.stop();*/
  }

  @Override
  public void getEvent(String str) {
    log.info("================ {}", str);
  }
}
