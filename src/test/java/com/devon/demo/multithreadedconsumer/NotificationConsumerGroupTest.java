package com.devon.demo.multithreadedconsumer;

import org.junit.Test;

/**
 * Created by diwenlao on 3/23/17.
 */
public class NotificationConsumerGroupTest {

    @Test
    public void testNotificationConsumerGroupTest() {
        String brokers = "172.16.143.138:9092";
        String groupId = "multi_threaded_consumer_test_group";
        String topic = "dedup";
        int numberOfConsumer = 10;


        // Start group of Notification Consumers
        NotificationConsumerGroup consumerGroup =
                new NotificationConsumerGroup(brokers, groupId, topic, numberOfConsumer);

        consumerGroup.execute();

        try {
            Thread.sleep(100000);
        } catch (InterruptedException ie) {
            ie.printStackTrace();
        }
    }
}
