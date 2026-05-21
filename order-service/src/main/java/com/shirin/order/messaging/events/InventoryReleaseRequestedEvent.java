package com.shirin.order.messaging.events;

import java.util.UUID;

public record InventoryReleaseRequestedEvent(
        UUID eventId,
        UUID orderId) {
}
