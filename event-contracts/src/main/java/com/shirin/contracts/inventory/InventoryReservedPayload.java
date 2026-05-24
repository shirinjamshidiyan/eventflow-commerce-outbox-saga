package com.shirin.contracts.inventory;

import java.util.UUID;

public record InventoryReservedPayload (
        UUID orderId
){
}
