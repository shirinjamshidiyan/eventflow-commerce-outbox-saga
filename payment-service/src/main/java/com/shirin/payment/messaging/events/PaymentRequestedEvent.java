package com.shirin.payment.messaging.events;

import jakarta.validation.constraints.*;

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
