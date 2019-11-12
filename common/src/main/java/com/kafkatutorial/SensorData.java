package com.kafkatutorial;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor // needed for json deserialization
public class SensorData {
    private String sensorId;
    private LocalDateTime timestamp;
    private double value;
}
