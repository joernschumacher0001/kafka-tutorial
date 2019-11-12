package com.kafkatutorial.roomsensors;

import com.kafkatutorial.SensorData;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class RoomSensorService {
    public static final String TOPIC_NAME = "roomSensorData";
    private Map<String, Set<String>> sensorsInRoom = new HashMap<>();

    private final Producer<String, SensorData> kafkaProducer;

    public RoomSensorService() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        kafkaProducer = new KafkaProducer<>(configs);
    }

    void assignSensor(String roomId, String sensorId) {
        sensorsInRoom.values().forEach(s -> s.remove(sensorId));
        sensorsInRoom.computeIfAbsent(roomId, id -> new HashSet<>()).add(sensorId);
    }

    void addSensorData(SensorData sensorData) {
        getRoomForSensor(sensorData.getSensorId())
                .ifPresent(roomId -> {
                    ProducerRecord<String, SensorData> record = new ProducerRecord<>(TOPIC_NAME, roomId, sensorData);
                    kafkaProducer.send(record);
                });
    }

    Optional<String> getRoomForSensor(String sensorId) {
        return sensorsInRoom.entrySet().stream()
                .filter(e -> e.getValue().contains(sensorId))
                .findFirst()
                .map(Map.Entry::getKey);
    }
}
