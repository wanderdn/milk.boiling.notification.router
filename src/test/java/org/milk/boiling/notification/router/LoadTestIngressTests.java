package org.milk.boiling.notification.router;

import org.junit.jupiter.api.Test;
import org.milk.boiling.notification.dto.MilkBoilingEvent;
import org.milk.boiling.notification.router.loadtest.*;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class LoadTestIngressTests {
    @Test
    void reportsPartialBatchBackpressureRatherThanClaimingSuccess() {
        var handler = mock(LoadTestWebSocketHandler.class);
        var first = new MilkBoilingEvent(UUID.randomUUID(), UUID.randomUUID(), "one");
        var second = new MilkBoilingEvent(UUID.randomUUID(), UUID.randomUUID(), "two");
        when(handler.offer(first)).thenReturn(true);
        when(handler.offer(second)).thenReturn(false);

        var result = new LoadTestIngressController(handler).publish(List.of(first, second));

        assertThat(result.getStatusCode().value()).isEqualTo(503);
        assertThat(result.getBody()).isEqualTo(1);
    }

    @Test
    void countsSuccessfullyAcceptedEvents() {
        var handler = mock(LoadTestWebSocketHandler.class);
        var event = new MilkBoilingEvent(UUID.randomUUID(), UUID.randomUUID(), "one");
        when(handler.offer(event)).thenReturn(true);

        var result = new LoadTestIngressController(handler).publish(List.of(event));

        assertThat(result.getStatusCode().value()).isEqualTo(200);
        assertThat(result.getBody()).isEqualTo(1);
    }
}
