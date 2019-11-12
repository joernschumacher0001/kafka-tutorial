package com.kafkatutorial.evaluation;

import com.kafkatutorial.SensorData;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.stream.Collectors.toList;

@Service
@Slf4j
public class EvaluationService {
    private static final String TOPIC_NAME = "roomSensorData";
    Map<String, List<Double>> dataForRoom = new HashMap<>();

    private final Consumer<String, SensorData> consumer;

    final ScheduledExecutorService kafkaExecutor = Executors.newScheduledThreadPool(3);

    EvaluationService() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, "evalution-service");
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        configs.put(JsonDeserializer.TRUSTED_PACKAGES, "com.kafkatutorial");
        consumer = new KafkaConsumer<>(configs);
    }

    double getRoomAverage(String roomId) {
        return dataForRoom.get(roomId).stream().mapToDouble(Double::doubleValue).average().getAsDouble();
    }

    @PostConstruct
    void startConsumer() {
        List<TopicPartition> partitions = consumer.partitionsFor(TOPIC_NAME)
                .stream()
                .map(pi -> new TopicPartition(TOPIC_NAME, pi.partition()))
                .collect(toList());
        System.out.printf("assigning to %d partitions%n", partitions.size());
        consumer.assign(partitions);
        kafkaExecutor.scheduleAtFixedRate(() -> {
            try {
                ConsumerRecords<String, SensorData> records = consumer.poll(Duration.ofSeconds(5));
                log.info("Read {} records", records.count());
                records.forEach(record -> {
                    String roomId = record.key();
                    SensorData value = record.value();
                    log.info("record for room {}: {}", roomId, value);
                    dataForRoom.computeIfAbsent(roomId, k -> new ArrayList<>()).add(value.getValue());
                });
            } catch (RuntimeException x) {
                log.error("error reading from kafka", x);
            }
        }, 0, 5, TimeUnit.SECONDS);
    }

    @PreDestroy
    void cleanup() {
        log.info("closing consumer");
        consumer.close();
    }
}
