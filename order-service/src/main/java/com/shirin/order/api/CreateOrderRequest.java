package com.shirin.order.api;

import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

public record CreateOrderRequest(

        @NotNull UUID requestId,
        @NotNull UUID checkoutId,
        @NotNull UUID customerId,
        @NotNull UUID paymentMethodId,
        @NotBlank String currency,
        @NotNull @DecimalMin("0.00") BigDecimal totalAmount,
        @NotEmpty List<@Valid OrderItemRequest> items

) {
}

