package com.shirin.inventory.messaging.events;

import java.util.UUID;

public record InventoryReservationFailedEvent(
        UUID eventId,
        UUID orderId,
        String reason
) {
}
