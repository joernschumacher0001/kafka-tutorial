package com.kafkatutorial.sensors;

import com.kafkatutorial.SensorData;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController("/")
public class SensorDataController {
    private final SensorDataService service;

    SensorDataController(SensorDataService srv) {
        service = srv;
    }

    @PutMapping(path="sensors/{id}/values", params = {"value"})
    void valueMeasured(@PathVariable("id") String id, @RequestParam("value") double value) {
        service.dataReceived(id, value);
    }
}
