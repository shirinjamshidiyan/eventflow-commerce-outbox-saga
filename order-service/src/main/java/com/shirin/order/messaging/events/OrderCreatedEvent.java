package com.shirin.order.messaging.events;

import java.util.List;
import java.util.UUID;

public record OrderCreatedEvent(
        UUID eventId,
        UUID orderId,
        List<OrderCreatedEventItem> items) { }
