package com.shirin.inventory.messaging.events;

import java.util.UUID;

public record OrderCreatedEventItem(
        UUID skuId,
        int quantity) {
}
