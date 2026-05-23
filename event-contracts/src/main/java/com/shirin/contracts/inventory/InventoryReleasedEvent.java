package com.shirin.contracts.inventory;

import jakarta.validation.constraints.NotNull;

import java.util.UUID;

public record InventoryReleasedEvent(
        @NotNull UUID eventId,
        @NotNull UUID orderId
) {
}
