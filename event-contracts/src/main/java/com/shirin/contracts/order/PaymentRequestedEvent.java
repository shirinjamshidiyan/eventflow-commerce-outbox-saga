package com.shirin.contracts.order;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;
import java.util.UUID;

public record PaymentRequestedEvent(
        @NotNull UUID eventId,
        @NotNull UUID orderId,
        @NotNull UUID paymentMethodId,
        @NotBlank @Pattern(regexp = "^[A-Z]{3}$") String currency,
        @NotNull @Positive BigDecimal amount

) {
}
