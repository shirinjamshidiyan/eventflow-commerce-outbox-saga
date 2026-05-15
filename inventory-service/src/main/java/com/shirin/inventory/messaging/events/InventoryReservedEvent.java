package com.shirin.inventory.messaging.events;

import java.util.UUID;

public record InventoryReservedEvent (
    UUID eventId,
    UUID orderId
){
}
