package com.shirin.contracts.order;

import jakarta.validation.constraints.NotNull;

import java.util.UUID;

public record InventoryReleaseRequestedEvent(
        @NotNull UUID eventId,
        @NotNull UUID orderId) {
}
