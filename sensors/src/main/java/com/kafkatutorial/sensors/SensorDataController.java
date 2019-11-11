package com.kafkatutorial.sensors;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController("/")
public class SensorDataController {
    private final SensorDataService service;

    SensorDataController(SensorDataService srv) {
        service = srv;
    }

    @PutMapping(path="values", params = {"id", "value"})
    void valueMeasured(@RequestParam("id") String id, @RequestParam("value") double value) {
        service.dataReceived(id, value);
    }
}
