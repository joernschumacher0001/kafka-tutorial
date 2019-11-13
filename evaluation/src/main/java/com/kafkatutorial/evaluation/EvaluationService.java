package com.kafkatutorial.evaluation;

import com.kafkatutorial.SensorData;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.net.URL;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class EvaluationService {

    @Value("${tutorial.room-sensor-service.url}")
    private String roomSensorServiceUrl;

    @Value("${tutorial.sensor-service.url}")
    private String sensorServiceUrl;

    private final RestTemplate restTemplate = new RestTemplate();

    double getRoomAverage(String roomId) {
        String sensorQueryUrl = String.format("%s/rooms/%s/sensors", roomSensorServiceUrl, roomId);
        String[] sensors = restTemplate.getForObject(sensorQueryUrl, String[].class);
        return sensors != null
                ? Arrays.stream(sensors).map(this::getSensorData)
                    .flatMapToDouble(l -> l.stream().mapToDouble(SensorData::getValue))
                    .average().orElse(0.0)
                : 0.0;
    }

    private List<SensorData> getSensorData(String sensorId) {
        String dataQueryUrl = String.format("%s/sensors/%s/values", sensorServiceUrl, sensorId);
        SensorData[] sensorData = restTemplate.getForObject(dataQueryUrl, SensorData[].class);
        return sensorData != null
                ? Arrays.stream(sensorData).collect(Collectors.toList())
                : Collections.emptyList();
    }
}
