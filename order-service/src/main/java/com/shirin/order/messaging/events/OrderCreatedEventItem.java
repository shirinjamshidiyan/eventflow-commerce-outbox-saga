package com.shirin.order.messaging.events;

import java.util.UUID;

public record OrderCreatedEventItem(
        UUID skuId,
        int quantity) {
}
