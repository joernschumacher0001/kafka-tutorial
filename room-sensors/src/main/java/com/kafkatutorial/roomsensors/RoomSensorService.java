package com.kafkatutorial.roomsensors;

import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class RoomSensorService {
    private Map<String, Set<String>> sensorsInRoom = new HashMap<>();

    void assignSensor(String roomId, String sensorId) {
        // avoid assigning the sensor to more than one room
        sensorsInRoom.values().forEach(s -> s.remove(sensorId));
        sensorsInRoom.computeIfAbsent(roomId, id -> new HashSet<>()).add(sensorId);
    }

    List<String> getSensorsForRoom(String roomId) {
        return new ArrayList<>(sensorsInRoom.getOrDefault(roomId, Collections.emptySet()));
    }
}
