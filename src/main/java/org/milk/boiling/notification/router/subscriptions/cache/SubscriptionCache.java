package org.milk.boiling.notification.router.subscriptions.cache;

import org.milk.boiling.notification.router.subscriptions.UserSubscriptions;

import java.util.List;
import java.util.UUID;

public interface SubscriptionCache {

    public List<String> getSubscriptionPods(UUID uuid);

    public UserSubscriptions updateSubscriptionPods(UUID uuid);
}
