package com.shirin.order.messaging.events;

import java.math.BigDecimal;
import java.util.UUID;

public record PaymentRequestedEvent(
        UUID eventId,
        UUID orderId,
        UUID paymentMethodId,
        String currency,
        BigDecimal amount

) {
}
