package com.kafkatutorial.evaluation;

import com.kafkatutorial.SensorData;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
public class EvaluationService {

    @Value("${kafka.topics.sensorData}")
    private String sensorDataTopic;

    @Value("${kafka.topics.sensorRoomAssignment}")
    private String sensorRoomAssignmentTopic;

    double getRoomAverage(String roomId) {
        Collection<String> sensors = new ArrayList<>();
        try (Consumer<String, String> assignmentReader =
                     createAndAssignConsumer(StringDeserializer.class, sensorRoomAssignmentTopic)) {
            ConsumerRecords<String, String> poll = assignmentReader.poll(Duration.ofSeconds(5));
            poll.forEach(cr -> {
                if (cr.key().equals(roomId)) {
                    sensors.add(cr.value());
                }
            });
        }

        log.info("Querying data for sensors {}", sensors);
        return getSensorData(sensors).stream()
                .mapToDouble(SensorData::getValue)
                .average()
                .orElse(0.0);
    }

    private List<SensorData> getSensorData(Collection<String> sensorIds) {
        List<SensorData> data = new ArrayList<>();

        try (Consumer<String, SensorData> sensorDataConsumer =
                     createAndAssignConsumer(JsonDeserializer.class, sensorDataTopic)) {
            ConsumerRecords<String, SensorData> records = sensorDataConsumer.poll(Duration.ofSeconds(10));
            records.forEach(cr -> {
                if (sensorIds.contains(cr.key())) {
                    data.add(cr.value());
                }
            });
        }
        return data;
    }

    private <T> Consumer<String, T> createAndAssignConsumer(Class<?> valueDeserializer, String topic) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, "evalution-service");
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        configs.put(JsonDeserializer.TRUSTED_PACKAGES, "com.kafkatutorial");
        KafkaConsumer<String, T> consumer = new KafkaConsumer<>(configs);

        List<TopicPartition> topicPartitions = getTopicPartitions(consumer, topic);
        consumer.assign(topicPartitions);

        // read starting at beginning of topic; otherwise, new calls will not receive
        // data that was already previously read
        consumer.seekToBeginning(topicPartitions);

        return consumer;
    }

    private List<TopicPartition> getTopicPartitions(Consumer<?, ?> sensorDataConsumer, String topic) {
        return sensorDataConsumer.partitionsFor(topic).stream()
                .mapToInt(PartitionInfo::partition)
                .mapToObj(i -> new TopicPartition(topic, i))
                .collect(Collectors.toList());
    }
}
