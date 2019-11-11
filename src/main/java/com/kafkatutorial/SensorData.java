package com.kafkatutorial;

import lombok.Data;

import java.time.LocalDateTime;

@Data
public class SensorData {
    private String sensorId;
    private LocalDateTime timestamp;
    private double value;
}
