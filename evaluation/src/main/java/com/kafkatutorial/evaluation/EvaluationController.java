package com.kafkatutorial.evaluation;

import com.kafkatutorial.SensorData;
import org.springframework.web.bind.annotation.*;

@RestController("/")
public class EvaluationController {
    private final EvaluationService service;

    public EvaluationController(EvaluationService service) {
        this.service = service;
    }

    @PutMapping("/data")
    void addSensorData(@RequestBody SensorData data) {
        service.addSensorData(data);
    }

    @GetMapping(value = "/average", params = "roomId")
    double getRoomAverage(@RequestParam("roomId") String roomId) {
        return service.getRoomAverage(roomId);
    }
}
