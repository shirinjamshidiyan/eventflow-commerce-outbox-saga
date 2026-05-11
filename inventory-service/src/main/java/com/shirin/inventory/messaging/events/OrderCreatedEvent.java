package com.shirin.inventory.messaging.events;

import java.util.List;
import java.util.UUID;

//todo2: هماهنگی ایونت ها در سرویس های مختلف

public record OrderCreatedEvent(
        UUID eventId,
        UUID orderId,
        List<OrderCreatedEventItem> items) { }
