package com.shirin.order.application;

import java.util.UUID;

public record CreateOrderResult(
        UUID orderId,
        boolean duplicate) {
}
