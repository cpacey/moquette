package io.moquette.spi.impl;

import io.moquette.parser.proto.messages.AbstractMessage;
import io.moquette.parser.proto.messages.PublishMessage;
import io.netty.channel.Channel;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.*;

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

    public List<PublishMessage> poll(long timeout) {
        ConsumerRecords<String, String> records = this.getRecords(timeout);
        List<PublishMessage> list = getPublishMessages(records);
        return list;
    }

    private ConsumerRecords<String, String> getRecords(long timeout) {
        synchronized (lock) {
            if (!subscribedTopics.isEmpty()) {
                return consumer.poll(timeout);
            } else {
                try {
                    Thread.sleep(timeout);
                } catch( InterruptedException e ) {
                    // Barf
                    Thread.currentThread().interrupt();
                }
                return ConsumerRecords.empty();
            }
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

    public void subscribeToAdditionalTopic(String topic) {
        synchronized (lock) {
            subscribedTopics.add(KafkaBackend.encodeMqttTopicToKafkaTopic(topic));
            updateConsumerSubscriptions();
        }
    }

    public void unsubscribeFromTopic(String topic) {
        synchronized (lock) {
            subscribedTopics.remove(KafkaBackend.encodeMqttTopicToKafkaTopic(topic));
            updateConsumerSubscriptions();
        }
    }


    private void updateConsumerSubscriptions() {
        Map<TopicPartition, Long> partitionPositions = getAllTopicPartitionPositions();

        consumer.subscribe(new ArrayList<>(subscribedTopics));

        setTopicPartitionPositions(partitionPositions);
    }

    private Map<TopicPartition, Long> getAllTopicPartitionPositions() {
        Set<String> activeTopics = consumer.subscription();

        Map<TopicPartition, Long> partitionPositions = new HashMap<>();
        for( String topic : activeTopics ) {
            List<PartitionInfo> partitions = consumer.partitionsFor( topic );

            for( PartitionInfo partitionInfo : partitions ) {
                TopicPartition topicPartition = new TopicPartition( partitionInfo.topic(), partitionInfo.partition() );

                long position = consumer.position( topicPartition );

                partitionPositions.put( topicPartition, position );
            }
        }
        return partitionPositions;
    }

    private void setTopicPartitionPositions(Map<TopicPartition, Long> partitionPositions) {
        partitionPositions.forEach( (key, value ) -> {
            consumer.seek( key, value.longValue() );
        } );
    }
}
