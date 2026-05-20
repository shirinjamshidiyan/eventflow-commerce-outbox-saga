package com.shirin.inventory.messaging.events;

import java.util.UUID;

public record InventoryReleasedEvent(
        UUID eventId,
        UUID orderId
) {
}
