package org.milk.boiling.notification.router.loadtest;

import org.milk.boiling.notification.dto.MilkBoilingEvent;
import org.milk.boiling.notification.router.rsocket.RSocketEventController;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@Profile("load-test")
public class RSocketLoadTestIngressController {
    private final RSocketEventController events;

    public RSocketLoadTestIngressController(RSocketEventController events) {
        this.events = events;
    }

    @PostMapping("/load/rsocket/events")
    public ResponseEntity<Integer> publish(@RequestBody List<MilkBoilingEvent> batch) {
        if (batch.size() > 5000) return ResponseEntity.badRequest().build();
        int accepted = 0;
        for (var event : batch) {
            if (!events.offer(event)) return ResponseEntity.status(503).body(accepted);
            accepted++;
        }
        return ResponseEntity.ok(accepted);
    }

    @GetMapping("/load/rsocket/stats")
    public java.util.Map<String, Long> stats() {
        return events.stats();
    }
}
