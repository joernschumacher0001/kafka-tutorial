package com.kafkatutorial;

import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@Builder
public class SensorData {
    private String sensorId;
    private LocalDateTime timestamp;
    private double value;
}
