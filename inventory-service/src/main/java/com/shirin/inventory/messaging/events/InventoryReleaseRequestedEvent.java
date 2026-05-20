package com.shirin.inventory.messaging.events;

import jakarta.validation.constraints.NotNull;

import java.util.UUID;

public record InventoryReleaseRequestedEvent(
        @NotNull UUID eventId,
        @NotNull UUID orderId
) {
}
