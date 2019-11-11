package com.kafkatutorial.sensors;

import com.kafkatutorial.SensorData;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.time.LocalDateTime;

@Service
public class SensorDataService {
    RestTemplate restTemplate = new RestTemplate();

    @Value("${tutorial.roomsensors.url}")
    private String targetUrl;

    public void dataReceived(String sensorId, double value) {

        SensorData data = SensorData.builder()
                .sensorId(sensorId)
                .timestamp(LocalDateTime.now())
                .value(value)
                .build();

        restTemplate.put(targetUrl, data);
    }
}
