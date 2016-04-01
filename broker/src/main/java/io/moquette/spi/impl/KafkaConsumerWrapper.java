package io.moquette.spi.impl;

import io.moquette.parser.proto.messages.AbstractMessage;
import io.moquette.parser.proto.messages.PublishMessage;
import io.netty.channel.Channel;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class KafkaConsumerWrapper {

    private final Consumer<String, String> consumer;
    private Channel channel;
    private final Set<String> subscribedTopics;
    private final Object lock;

    public KafkaConsumerWrapper(Consumer<String, String> consumer, Channel channel) {
        this.consumer = consumer;
        this.channel = channel;
        this.subscribedTopics = new HashSet<>();
        this.lock = new Object();

        new Thread(() -> {
            while (true) {
                List<PublishMessage> messages = this.poll(100);
                if (!messages.isEmpty()) {
                    messages.forEach(channel::write);
                    channel.flush();
                }
            }
        }).start();
    }

    private ConsumerRecords<String, String> pollInternal(long timeout) {
        synchronized (lock) {
            if (!subscribedTopics.isEmpty()) {
                return consumer.poll(timeout);
            } else {
                return ConsumerRecords.empty();
            }
        }
    }

    public List<PublishMessage> poll(long timeout) {
        ConsumerRecords<String, String> records = this.pollInternal(timeout);

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

    public void subscribeToAdditionalTopic(String topic) {
        synchronized (lock) {
            subscribedTopics.add(KafkaBackend.encodeMqttTopicToKafkaTopic(topic));
            consumer.subscribe(new ArrayList<>(subscribedTopics));
        }
    }

    public void unsubscribeFromTopic(String topic) {
        synchronized (lock) {
            subscribedTopics.remove(KafkaBackend.encodeMqttTopicToKafkaTopic(topic));
            consumer.subscribe(new ArrayList<>(subscribedTopics));
        }
    }
}
