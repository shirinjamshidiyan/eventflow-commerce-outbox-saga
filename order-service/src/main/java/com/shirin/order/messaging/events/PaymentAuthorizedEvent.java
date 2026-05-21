package com.shirin.order.messaging.events;

import jakarta.validation.constraints.NotNull;

import java.util.UUID;

public record PaymentAuthorizedEvent(
        @NotNull UUID eventId,
        @NotNull UUID orderId,
        @NotNull UUID paymentId
) {
}
