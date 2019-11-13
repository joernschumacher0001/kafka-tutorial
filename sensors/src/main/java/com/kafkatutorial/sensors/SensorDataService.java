package com.kafkatutorial.sensors;

import com.kafkatutorial.SensorData;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.*;

@Service
public class SensorDataService {
    private final Producer<String, SensorData> kafkaProducer;

    @Value("${kafka.topic.sensordata}")
    private String sensorDataTopic;

    public SensorDataService() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        kafkaProducer = new KafkaProducer<>(configs);
    }

    void dataReceived(String sensorId, double value) {
        SensorData data = SensorData.builder()
                .sensorId(sensorId)
                .timestamp(LocalDateTime.now())
                .value(value).build();
        kafkaProducer.send(new ProducerRecord<>(sensorDataTopic, sensorId, data));
    }
}
