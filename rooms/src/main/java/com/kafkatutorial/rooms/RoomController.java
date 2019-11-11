package com.kafkatutorial.rooms;

import com.kafkatutorial.RoomData;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController("/")
public class RoomController {
    private final RoomService roomService;

    public RoomController(RoomService roomService) {
        this.roomService = roomService;
    }

    @PutMapping(value = "rooms")
    void addRoom(@RequestBody RoomData room) {
        roomService.roomAdded(room);
    }
}
