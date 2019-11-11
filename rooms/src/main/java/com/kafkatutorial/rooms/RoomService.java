package com.kafkatutorial.rooms;

import com.kafkatutorial.RoomData;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;

@Service
public class RoomService {
    RestTemplate template = new RestTemplate();

    private final Map<String, RoomData> rooms = new HashMap<>();

    void roomAdded(RoomData room) {
        rooms.put(room.getId(), room);
    }

    RoomData getRoom(String id) {
        return rooms.get(id);
    }
}
