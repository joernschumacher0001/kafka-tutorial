package com.kafkatutorial.roomsensors;

import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController("/")
public class RoomSensorController {

    private final RoomSensorService roomSensorService;

    public RoomSensorController(RoomSensorService roomSensorService) {
        this.roomSensorService = roomSensorService;
    }

    @PutMapping(value = "rooms/{roomId}/sensors/{sensorId}")
    void assignSensorToRoom(@PathVariable("roomId") String roomId, @PathVariable("sensorId") String sensorId) {
        roomSensorService.assignSensor(roomId, sensorId);
    }

    @GetMapping(value = "rooms/{roomId}/sensors")
    List<String> getSensorsForRoom(@PathVariable("roomId") String roomId) {
        return roomSensorService.getSensorsForRoom(roomId);
    }
}
