package com.kafkatutorial.roomsensors;

import com.kafkatutorial.SensorData;
import org.springframework.web.bind.annotation.*;

@RestController("/")
public class RoomSensorController {

    private final RoomSensorService roomSensorService;

    public RoomSensorController(RoomSensorService roomSensorService) {
        this.roomSensorService = roomSensorService;
    }

    @PutMapping(value = "sensors", params = {"roomId", "sensorId"})
    void assignSensorToRoom(@RequestParam("roomId") String roomId, @RequestParam("sensorId") String sensorId) {
        roomSensorService.assignSensor(roomId, sensorId);
    }

    @PutMapping(value = "sensorData")
    void putSensorData(@RequestBody SensorData sensorData) {
        roomSensorService.addSensorData(sensorData);
    }

    @GetMapping(value = "roomForSensor", params = "sensorId")
    String getRoomForSensor(@RequestParam("sensorId") String sensorId) {
        return roomSensorService.getRoomForSensor(sensorId).orElse("unknown");
    }
}
