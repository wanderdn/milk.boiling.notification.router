package org.milk.boiling.notification.router.subscriptions;

import java.util.List;
import java.util.UUID;

public record UserSubscriptions(UUID userId, List<String> sessions) {
}
