package com.kafkatutorial.roomsensors;

import com.kafkatutorial.SensorData;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.*;

@Service
public class RoomSensorService {
    private Map<String, Set<String>> sensorsInRoom = new HashMap<>();

    private final RestTemplate restTemplate = new RestTemplate();

    @Value("${tutorial.evaluation.url}")
    private String evaluationUrl;

    void assignSensor(String roomId, String sensorId) {
        sensorsInRoom.values().forEach(s -> s.remove(sensorId));
        sensorsInRoom.computeIfAbsent(roomId, id -> new HashSet<>()).add(sensorId);
    }

    void addSensorData(SensorData sensorData) {
        String url = String.format("%s/data", evaluationUrl);
        restTemplate.put(url, sensorData);
    }

    String getRoomForSensor(String sensorId) {
        return sensorsInRoom.entrySet().stream()
                .filter(e -> e.getValue().contains(sensorId))
                .findFirst()
                .map(Map.Entry::getKey)
                .orElse("unknown");
    }
}
