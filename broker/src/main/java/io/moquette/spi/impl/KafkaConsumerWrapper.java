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
    private boolean subscribedTopicsDirty = false;
    private final Lock lock;
    private final Condition condition;

    public KafkaConsumerWrapper(Consumer<String, String> consumer) {
        this.consumer = consumer;
        this.subscribedTopics = new HashMap<>();
        this.lock = new ReentrantLock();
        this.condition = lock.newCondition();

        new Thread(() -> {
            while (true) {
                List<String> newSubscriptions = null;
                synchronized (subscribedTopics) {
                    while (subscribedTopics.isEmpty()) {
                        try {
                            subscribedTopics.wait();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }

                    if (subscribedTopicsDirty) {
                        newSubscriptions = computeSubscriptionList(subscribedTopics.keySet());
                        subscribedTopicsDirty = false;
                    }
                }

                if (newSubscriptions != null) {
                    consumer.subscribe(newSubscriptions);
                }

                ConsumerRecords<String, String> records = consumer.poll(100);

                if (!records.isEmpty()) {
                    List<PublishMessage> messages = getPublishMessages(records);
                    messages.forEach( m -> {
                        LOG.debug("PUBLISH from Kafka on topic {}", m.getTopicName());
                        Set<Channel> subscribers = null;
                        synchronized (subscribedTopics) {
                            Set<Channel> channels = subscribedTopics.get(m.getTopicName());
                            if (channels != null) {
                                subscribers = new HashSet(channels);
                            }
                        }

                        if (subscribers != null) {
                            subscribers.forEach(ch -> {
                                if (ch.isActive()) {
                                    ch.writeAndFlush(m);
                                }
                            });
                        }
                    } );
                }
            }
        }, "Kafka polling thread").start();
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
        synchronized (subscribedTopics) {
            Set<Channel> topicSubscribers = subscribedTopics.get(topic);

            if(topicSubscribers == null) {
                topicSubscribers = new HashSet<>();
                subscribedTopics.put(topic, topicSubscribers);

                subscribedTopicsDirty = true;

                topicSubscribers.add(channel);
            } else {
                topicSubscribers.add(channel);
            }
            subscribedTopics.notifyAll();
        }
    }

    public void unsubscribeFromTopic(String topic, Channel channel) {
        synchronized (subscribedTopics) {
            Set<Channel> topicSubscribers = subscribedTopics.get(topic);

            if(topicSubscribers == null) {
                return;
            }

            topicSubscribers.remove(channel);

            if( topicSubscribers.isEmpty() ) {
                subscribedTopics.remove(topic);
                subscribedTopicsDirty = true;
            }
        }
    }

    private static List<String> computeSubscriptionList(Set<String> strings) {
        ArrayList<String> list = new ArrayList<>();
        strings.forEach(s -> list.add(KafkaBackend.encodeMqttTopicToKafkaTopic(s)));
        return list;
    }
}
