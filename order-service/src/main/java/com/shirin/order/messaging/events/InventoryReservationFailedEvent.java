package com.shirin.order.messaging.events;

import java.util.UUID;

public record InventoryReservationFailedEvent(
        UUID eventId,
        UUID orderId,
        String reason
) {
}
