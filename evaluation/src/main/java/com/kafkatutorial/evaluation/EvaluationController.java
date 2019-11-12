package com.kafkatutorial.evaluation;

import org.springframework.web.bind.annotation.*;

@RestController("/")
public class EvaluationController {
    private final EvaluationService service;

    public EvaluationController(EvaluationService service) {
        this.service = service;
    }

    @GetMapping(value = "/average", params = "roomId")
    double getRoomAverage(@RequestParam("roomId") String roomId) {
        return service.getRoomAverage(roomId);
    }
}
