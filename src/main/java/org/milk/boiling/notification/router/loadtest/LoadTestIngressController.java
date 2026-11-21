package org.milk.boiling.notification.router.loadtest;

import org.milk.boiling.notification.dto.MilkBoilingEvent;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@Profile("load-test")
public class LoadTestIngressController {
    private final LoadTestWebSocketHandler handler;

    public LoadTestIngressController(LoadTestWebSocketHandler handler) {
        this.handler = handler;
    }

    @GetMapping("/load/ready")
    public String ready() {
        return "load-test";
    }

    @PostMapping("/load/events")
    public ResponseEntity<Integer> publish(@RequestBody List<MilkBoilingEvent> events) {
        if (events.size() > 5000) {
            return ResponseEntity.badRequest().build();
        }
        int accepted = 0;
        for (var event : events) {
            if (!handler.offer(event)) {
                return ResponseEntity.status(503).body(accepted);
            }
            accepted++;
        }
        return ResponseEntity.ok(accepted);
    }
}
