package com.flink.demo;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

public class MyRecordSerializer implements
        KafkaRecordSerializationSchema<User> {

    private static final long serialVersionUID = 1L;

    private String topic;

    public MyRecordSerializer() {}

    public MyRecordSerializer(String topic) {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            User element, KafkaSinkContext context, Long timestamp) {

        try {
            return new ProducerRecord<byte[], byte[]>(
                    topic,
                    element.getUserid().getBytes(),
                    Utils.getMapper().writeValueAsBytes(element));
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(
                    "Could not serialize record: " + element, e);
        }
    }
}