package com.shirin.order.api;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.util.UUID;

public record OrderItemRequest(
        @NotNull UUID skuId,
        @Positive int quantity) { }
