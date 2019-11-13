package com.kafkatutorial.sensors;

import com.kafkatutorial.SensorData;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.time.LocalDateTime;
import java.util.*;

@Service
public class SensorDataService {
    RestTemplate restTemplate = new RestTemplate();

    private final Map<String, List<SensorData>> data = new HashMap<>();

    void dataReceived(String sensorId, double value) {
        SensorData data = SensorData.builder()
                .sensorId(sensorId)
                .timestamp(LocalDateTime.now())
                .value(value).build();
        this.data.computeIfAbsent(sensorId, s -> new ArrayList<>())
                .add(data);
    }

    List<SensorData> getDataForSensor(String sensorId) {
        return data.getOrDefault(sensorId, Collections.emptyList());
    }
}
