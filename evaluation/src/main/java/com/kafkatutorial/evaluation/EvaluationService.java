package com.kafkatutorial.evaluation;

import com.kafkatutorial.SensorData;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;

@Slf4j
@Service
class EvaluationService {

    @Value("${kafka.topics.sensorData}")
    private String sensorDataTopic;

    @Value("${kafka.topics.sensorRoomAssignment}")
    private String sensorRoomAssignmentTopic;

    @Value("${kafka.windowsize}")
    private int windowSize;

    private ReadOnlyWindowStore<String, Double> averageByRoom;
    private KafkaStreams kafkaStreams;
    private Serde<SensorData> jsonSerde;

    EvaluationService() {
        JsonDeserializer<SensorData> deserializer = new JsonDeserializer<>();
        deserializer.addTrustedPackages("com.kafkatutorial");
        jsonSerde = Serdes.serdeFrom(new JsonSerializer<>(), deserializer);
    }

    double getRoomAverage(String roomId) {
        double result = 0.0;
        if (averageByRoom != null) {
            Instant windowStart = Instant.now().minus(2*windowSize, ChronoUnit.MINUTES);
            try (WindowStoreIterator<Double> fetch = averageByRoom.fetch(roomId, windowStart, Instant.now())) {
                while (fetch.hasNext()) {
                    result = fetch.next().value;
                }
            }
        }
        return result;
    }

    @PostConstruct
    void createStream() {
        TimeWindows timeWindow = TimeWindows.of(Duration.ofMinutes(windowSize));

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // sensorid to roomid
        KTable<String, String> sensorRoomTable = streamsBuilder.table(sensorRoomAssignmentTopic);

        KStream<String, SensorData> valueStream =
                streamsBuilder.stream(sensorDataTopic, Consumed.with(Serdes.String(), jsonSerde));

        @Data
        class RoomSensorData {
            final String roomId;
            final SensorData sensorData;
        }
        KStream<String, SensorData> roomSensorData =
                valueStream.leftJoin(sensorRoomTable, (sd, r) -> new RoomSensorData(r, sd), Joined.valueSerde(jsonSerde))
                        .map((k, rsd) -> new KeyValue<>(rsd.roomId, rsd.sensorData));

        Materialized<String, Double, WindowStore<Bytes, byte[]>> averageByRoom1 =
                Materialized.<String, Double, WindowStore<Bytes, byte[]>>as("averageByRoom")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Double());
        TimeWindowedKStream<String, SensorData> kGroupedStream = roomSensorData
                .groupByKey(Grouped.with(Serdes.String(), jsonSerde))
                .windowedBy(timeWindow);
        kGroupedStream.aggregate(() -> Double.valueOf(0), new AverageAggregator(),
                        averageByRoom1);

        Topology topology = streamsBuilder.build();
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "evaluation-service");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        kafkaStreams = new KafkaStreams(topology, properties);

        kafkaStreams.setStateListener(new RunningListener());
        kafkaStreams.start();
    }

    private class RunningListener implements KafkaStreams.StateListener {
        @Override
        public void onChange(KafkaStreams.State newState, KafkaStreams.State oldState) {
            if (newState == KafkaStreams.State.RUNNING
                    && oldState != KafkaStreams.State.CREATED
                    && averageByRoom == null) {
                try {
                    while (averageByRoom == null) {
                        try {
                            log.info("retrieving store");
                            averageByRoom = kafkaStreams.store("averageByRoom", QueryableStoreTypes.windowStore());
                            log.info("state store initialized");
                        } catch (InvalidStateStoreException x) {
                            Thread.sleep(1000);
                        }
                    }
                } catch (Exception x) {
                    log.error("failed to retrieve store", x);
                }
            }
        }
    }

    @PreDestroy
    void cleanup() {
        if (kafkaStreams != null) {
            kafkaStreams.close();
        }
    }
}

class AverageAggregator implements Aggregator<String, SensorData, Double> {
    private double sum;
    private int count;

    @Override
    public Double apply(String s, SensorData sensorData, Double aDouble) {
        sum += sensorData.getValue();
        return (sum / (double) ++count);
    }
}