package com.kafkatutorial.evaluation;

import com.kafkatutorial.SensorData;
import org.springframework.web.bind.annotation.*;

@RestController("/")
public class EvaluationController {
    private final EvaluationService service;

    public EvaluationController(EvaluationService service) {
        this.service = service;
    }

    @GetMapping(value = "rooms/{roomId}/average")
    double getRoomAverage(@PathVariable("roomId") String roomId) {
        return service.getRoomAverage(roomId);
    }
}
