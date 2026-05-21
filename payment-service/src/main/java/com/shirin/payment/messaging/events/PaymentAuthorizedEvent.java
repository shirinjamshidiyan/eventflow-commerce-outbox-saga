package com.shirin.payment.messaging.events;

import java.util.UUID;

public record PaymentAuthorizedEvent(
        UUID eventId,
        UUID orderId,
        UUID paymentId
) {
}
