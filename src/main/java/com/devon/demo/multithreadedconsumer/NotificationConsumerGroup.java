package com.devon.demo.multithreadedconsumer;

/**
 * Created by diwenlao on 3/23/17.
 */

import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.ArrayList;
import java.util.List;

public class NotificationConsumerGroup {

    private final int numberOfConsumers;
    private final String groupId;
    private final String topic;
    private final String brokers;
    private List<NotificationConsumerThread> consumers;
    private ThreadPoolTaskExecutor tpte = new ThreadPoolTaskExecutor();

    public NotificationConsumerGroup(String brokers, String groupId, String topic,
                                     int numberOfConsumers) {
        this.brokers = brokers;
        this.topic = topic;
        this.groupId = groupId;
        this.numberOfConsumers = numberOfConsumers;
        consumers = new ArrayList<>();
        for (int i = 0; i < this.numberOfConsumers; i++) {
            NotificationConsumerThread ncThread =
                    new NotificationConsumerThread(this.brokers, this.groupId, this.topic);
            consumers.add(ncThread);
        }
        tpte.setCorePoolSize(1);
        tpte.setMaxPoolSize(numberOfConsumers);
        tpte.setQueueCapacity(5);
        tpte.initialize();
    }

    public void execute() {

        for (NotificationConsumerThread ncThread : consumers) {
//            Thread t = new Thread(ncThread);
//            t.start();
            try {
                tpte.execute(ncThread);
                Thread.sleep(6000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }

    /**
     * @return the numberOfConsumers
     */
    public int getNumberOfConsumers() {
        return numberOfConsumers;
    }

    /**
     * @return the groupId
     */
    public String getGroupId() {
        return groupId;
    }

}
