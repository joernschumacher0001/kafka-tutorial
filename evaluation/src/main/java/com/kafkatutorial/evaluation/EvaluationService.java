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

    private static final String AVERAGE_BY_ROOM = "averageByRoom";

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
        GlobalKTable<String, String> sensorRoomTable = streamsBuilder.globalTable(sensorRoomAssignmentTopic);

        KStream<String, SensorData> valueStream =
                streamsBuilder.stream(sensorDataTopic, Consumed.with(Serdes.String(), jsonSerde));

        // intermediate type to store roomId->sensorData
        @Data
        class RoomSensorData {
            final String roomId;
            final SensorData sensorData;
        }
        // map key/value pair from value stream to global table's key
        KeyValueMapper<String, SensorData, String> mapper = (s, sd) -> sd.getSensorId();
        // join a value from the stream with a key from the global table
        ValueJoiner<SensorData, String, RoomSensorData> joiner = (sd, gv) -> new RoomSensorData(gv, sd);
        // stream to roomId->sensor value
        KStream<String, Double> roomSensorData =
                valueStream.leftJoin(sensorRoomTable, mapper, joiner)
                        .map((k, rsd) -> new KeyValue<>(rsd.roomId, rsd.sensorData.getValue()));

        // declare the Serdes for the state store from the created stream
        Materialized<String, Double, WindowStore<Bytes, byte[]>> averageByRoom1 =
                Materialized.<String, Double, WindowStore<Bytes, byte[]>>as(AVERAGE_BY_ROOM)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Double());
        // fill the store based on our time window
        roomSensorData
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
                .windowedBy(timeWindow)
                .aggregate(() -> Double.valueOf(0), new AverageAggregator(),
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
            // the state is only available after the stream has started
            if (newState == KafkaStreams.State.RUNNING
                    && oldState != KafkaStreams.State.CREATED
                    && averageByRoom == null) {
                try {
                    while (averageByRoom == null) {
                        try {
                            log.info("retrieving store");
                            averageByRoom = kafkaStreams.store(AVERAGE_BY_ROOM, QueryableStoreTypes.windowStore());
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

class AverageAggregator implements Aggregator<String, Double, Double> {
    private double sum;
    private int count;

    @Override
    public Double apply(String s, Double sensorData, Double aDouble) {
        sum += sensorData;
        return (sum / (double) ++count);
    }
}