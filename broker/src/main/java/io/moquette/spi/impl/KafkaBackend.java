package io.moquette.spi.impl;

import io.moquette.parser.proto.messages.PublishMessage;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.UnsupportedEncodingException;
import java.util.*;

/**
 * Created by cpacey on 01/04/16.
 */
public class KafkaBackend {

    private Producer<String, String> m_producer;

    public void init() {
        m_producer = createKafkaProducer();
    }

    public void publish(PublishMessage msg) {
        String topicName = encodeMqttTopicToKafkaTopic(msg.getTopicName());
        String message;
        try {
            message = new String(msg.getPayload().array(), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        m_producer.send(new ProducerRecord<String, String>(topicName, message));
    }

    private static Producer<String, String> createKafkaProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "172.17.0.1:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<>(props);
        return producer;
    }

    private Consumer<String, String> createKafkaConsumerRaw() {
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", "172.17.0.1:9092");
        consumerProps.put("group.id", "group");
        consumerProps.put("enable.auto.commit", "true");
        consumerProps.put("auto.commit.interval.ms", "1000");
        consumerProps.put("session.timeout.ms", "30000");
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        Consumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        return consumer;
    }

    public KafkaConsumerWrapper createKafkaConsumer(io.netty.channel.Channel channel) {
        Consumer<String, String> consumer = createKafkaConsumerRaw();

        return new KafkaConsumerWrapper(consumer, channel);
    }

    public static String encodeMqttTopicToKafkaTopic(String rawTopic) {
        return rawTopic.replaceAll( "/", "_" );
    }

    public static String decodeMqttTopicFromKafkaTopic(String rawTopic) {
        return rawTopic.replaceAll( "_", "/" );
    }
}

