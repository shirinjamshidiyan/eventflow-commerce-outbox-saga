package com.shirin.order.application;

import java.util.UUID;

public record CreateOrderCommandItem(UUID skuId, int quantity) {
}
