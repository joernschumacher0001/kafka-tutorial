package com.kafkatutorial.evaluation;

import com.kafkatutorial.SensorData;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class EvaluationService {
    Map<String, List<Double>> dataForRoom = new HashMap<>();

    @Value("${tutorial.room-sensor-service.url}")
    private String roomSensorServiceUrl;

    private final RestTemplate restTemplate = new RestTemplate();

    void addSensorData(SensorData data) {
        String queryUrl = String.format("%s/roomForSensor?sensorId=%s", roomSensorServiceUrl, data.getSensorId());
        String roomId = restTemplate.getForObject(queryUrl, String.class);
        dataForRoom.computeIfAbsent(roomId, id -> new ArrayList<>()).add(data.getValue());
    }

    public double getRoomAverage(String roomId) {
        return dataForRoom.get(roomId).stream().mapToDouble(Double::doubleValue).average().getAsDouble();
    }
}
