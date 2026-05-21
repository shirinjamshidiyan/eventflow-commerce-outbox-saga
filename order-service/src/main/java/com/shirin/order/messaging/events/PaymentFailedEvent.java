package com.shirin.order.messaging.events;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.util.UUID;

public record PaymentFailedEvent(
         @NotNull UUID eventId,
         @NotNull UUID orderId,
         @NotNull UUID paymentId,
         @NotBlank String reason
) {
}
