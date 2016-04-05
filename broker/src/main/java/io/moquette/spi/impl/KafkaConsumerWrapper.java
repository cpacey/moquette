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
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class KafkaConsumerWrapper {

    private final Consumer<String, String> consumer;
    private final Set<String> subscribedTopics;
    private final Lock lock;
    private final Condition condition;
    private volatile boolean keepRunningThread = true;

    public KafkaConsumerWrapper(Consumer<String, String> consumer, Channel channel) {
        this.consumer = consumer;
        this.subscribedTopics = new HashSet<>();
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
                        messages.forEach(channel::write);
                        channel.flush();
                    }
                } finally {
                    lock.unlock();
                }
            }
            System.out.println("shutting down thread");
            consumer.close();
        }).start();
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

    public void subscribeToAdditionalTopic(String topic) {
        lock.lock();
        try {
            subscribedTopics.add(KafkaBackend.encodeMqttTopicToKafkaTopic(topic));
            updateConsumerSubscriptions();
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public void unsubscribeFromTopic(String topic) {
        lock.lock();
        try {
            subscribedTopics.remove(KafkaBackend.encodeMqttTopicToKafkaTopic(topic));
            updateConsumerSubscriptions();
        } finally {
            lock.unlock();
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
