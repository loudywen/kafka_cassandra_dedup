/*
package com.devon.demo.annotation_approch;

import java.util.concurrent.CountDownLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;

@SpringBootApplication
@EnableCassandraRepositories(basePackages = {"com.devon.demo.cassandra"})
@EnableKafka
public class demo2 {

  private Logger log = LoggerFactory.getLogger(demo2.class);
  private int count;

  public static void main(String[] args) throws InterruptedException {
    SpringApplication.run(demo2.class, args);

  }

  @Bean
  public Listener listener() {
    return new Listener();
  }


  public static class Listener {

    private final CountDownLatch latch = new CountDownLatch(3);

    private int count;

    @KafkaListener(topics = "dedup", containerFactory = "retryKafkaListenerContainerFactory")
    public void listen(String foo) {
      ++count;
      if (this.count < 3) {
        System.out.println("Received: " + foo);
        System.out.println("Success: " + foo);

      } else {
        System.out.println("failed: " + foo);

        throw new RuntimeException("retry");
      }

    }

  }

}
*/
