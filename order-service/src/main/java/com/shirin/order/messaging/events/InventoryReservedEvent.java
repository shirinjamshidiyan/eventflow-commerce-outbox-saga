package com.shirin.order.messaging.events;

import java.util.UUID;

public record InventoryReservedEvent (
    UUID eventId,
    UUID orderId
){}
