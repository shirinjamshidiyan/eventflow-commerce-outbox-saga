package com.shirin.contracts.inventory;

import java.util.UUID;

public record InventoryReservedEvent (
        UUID eventId,
        UUID orderId
){
}
