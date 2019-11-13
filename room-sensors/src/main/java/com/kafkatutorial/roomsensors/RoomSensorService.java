package com.kafkatutorial.roomsensors;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class RoomSensorService {
    private final Producer<String, String> kafkaProducer;

    @Value("${kafka.topics.sensorRoomAssignment}")
    private String sensorAssignmentTopic;

    public RoomSensorService() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        kafkaProducer = new KafkaProducer<>(configs);
    }

    void assignSensor(String roomId, String sensorId) {
        kafkaProducer.send(new ProducerRecord<>(sensorAssignmentTopic, roomId, sensorId));
    }
}
