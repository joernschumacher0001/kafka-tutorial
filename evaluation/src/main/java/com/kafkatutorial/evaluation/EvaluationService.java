package com.kafkatutorial.evaluation;

import com.kafkatutorial.SensorData;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class EvaluationService {
    Map<String, List<Double>> dataForRoom = new HashMap<>();

    @Value("${tutorial.roomsensors.url}")
    private String roomSensorServiceUrl;

    private final RestTemplate restTemplate = new RestTemplate();

    void addSensorData(SensorData data) {
        String queryUrl = String.format("%s/roomForSensor?sensorId=%s", roomSensorServiceUrl, data.getSensorId());
        log.info("querying {}", queryUrl);
        String roomId = restTemplate.getForObject(queryUrl, String.class);
        dataForRoom.computeIfAbsent(roomId, id -> new ArrayList<>()).add(data.getValue());
    }

    public double getRoomAverage(String roomId) {
        return dataForRoom.get(roomId).stream().mapToDouble(Double::doubleValue).average().getAsDouble();
    }
}
