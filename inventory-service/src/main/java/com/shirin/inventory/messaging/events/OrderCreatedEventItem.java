package com.shirin.inventory.messaging.events;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.util.UUID;

public record OrderCreatedEventItem(
        @NotNull UUID skuId,
        @Positive @NotNull Integer quantity) {
}
