package com.shirin.contracts.inventory;

import jakarta.validation.constraints.NotNull;

import java.util.UUID;

public record InventoryReleasedPayload(
        @NotNull UUID orderId
) {
}
