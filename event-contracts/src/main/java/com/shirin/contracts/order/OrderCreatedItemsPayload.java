package com.shirin.contracts.order;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.util.UUID;

public record OrderCreatedItemsPayload(
        @NotNull UUID skuId,
        @NotNull @Positive Integer quantity) {
}
