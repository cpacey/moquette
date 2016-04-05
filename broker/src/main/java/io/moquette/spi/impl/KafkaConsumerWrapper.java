package io.moquette.spi.impl;

import io.moquette.parser.proto.messages.AbstractMessage;
import io.moquette.parser.proto.messages.PublishMessage;
import io.netty.channel.Channel;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class KafkaConsumerWrapper {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerWrapper.class);

    private final Consumer<String, String> consumer;
    private final HashMap<String, Set<Channel>> subscribedTopics;
    private final Lock lock;
    private final Condition condition;
    private volatile boolean keepRunningThread = true;

    public KafkaConsumerWrapper(Consumer<String, String> consumer) {
        this.consumer = consumer;
        this.subscribedTopics = new HashMap<>();
        this.lock = new ReentrantLock();
        this.condition = lock.newCondition();

        new Thread(() -> {
            while (keepRunningThread) {
                lock.lock();

                try {
                    while (subscribedTopics.isEmpty()) {
                        condition.awaitUninterruptibly();
                    }

                    // Might have been woken up for shutdown.
                    if( !keepRunningThread ) {
                        break;
                    }

                    ConsumerRecords<String, String> records = consumer.poll(100);
                    List<PublishMessage> messages  = getPublishMessages(records);
                    if (!messages.isEmpty()) {
                        messages.forEach( m -> {
                            LOG.debug("PUBLISH from Kafka on topic {}", m.getTopicName());
                            Set<Channel> subscribers = subscribedTopics.get(m.getTopicName());
                            subscribers.forEach(ch -> {
                                if (ch.isActive()) {
                                    ch.writeAndFlush(m);
                                }
                            });
                        } );
                    }
                } finally {
                    lock.unlock();
                }
            }
            System.out.println("shutting down thread");
            consumer.close();
        }, "Kafka polling thread").start();
    }

    public void shutdown() {
        this.keepRunningThread = false;

        // This is terrible!  Shutdown should not block.
        lock.lock();
        try {
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private List<PublishMessage> getPublishMessages(ConsumerRecords<String, String> records) {
        List<PublishMessage> list = new ArrayList<>();

        for (ConsumerRecord<String, String> record : records) {

            PublishMessage publishMessage = new PublishMessage();

            publishMessage.setRetainFlag(false);

            String mqttTopic = KafkaBackend.decodeMqttTopicFromKafkaTopic(record.topic());
            publishMessage.setTopicName(mqttTopic);

            try {
                publishMessage.setPayload(ByteBuffer.wrap(record.value().getBytes("UTF-8")));
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }

            publishMessage.setQos(AbstractMessage.QOSType.MOST_ONE);

            list.add(publishMessage);
        }
        return list;
    }

    public void subscribeToAdditionalTopic(String topic, Channel channel) {
        lock.lock();
        try {
            Set<Channel> topicSubscribers = subscribedTopics.get(topic);

            if(topicSubscribers == null) {
                topicSubscribers = new HashSet<>();
                subscribedTopics.put(topic, topicSubscribers);

                topicSubscribers.add(channel);

                updateConsumerSubscriptions();
            } else {
                topicSubscribers.add(channel);
            }
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public void unsubscribeFromTopic(String topic, Channel channel) {
        lock.lock();
        try {
            Set<Channel> topicSubscribers = subscribedTopics.get(topic);

            if(topicSubscribers == null) {
                return;
            }

            topicSubscribers.remove(channel);

            if( topicSubscribers.isEmpty() ) {
                subscribedTopics.remove(topic);
                updateConsumerSubscriptions();
            }
        } finally {
            lock.unlock();
        }
    }

    private void updateConsumerSubscriptions() {
        List<String> subscriptions = computeSubscriptionList(subscribedTopics.keySet());
        consumer.subscribe(subscriptions);
    }

    private static List<String> computeSubscriptionList(Set<String> strings) {
        ArrayList<String> list = new ArrayList<>();
        strings.forEach(s -> list.add(KafkaBackend.encodeMqttTopicToKafkaTopic(s)));
        return list;
    }
}
