package org.milk.boiling.notification.router.subscriptions;

import java.util.UUID;

public record MilkBoilingEvent(UUID eventId, UUID userId, String payload) {
}
