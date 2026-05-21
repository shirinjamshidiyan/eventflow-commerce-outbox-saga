package com.shirin.payment.messaging.events;

import lombok.NonNull;

import java.util.UUID;

public record PaymentFailedEvent(
         UUID eventId,
         UUID orderId,
         UUID paymentId,
         String reason
) {
}
