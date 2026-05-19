package com.shirin.order.application;

import java.math.BigDecimal;
import java.util.UUID;

public record CreateOrderCommandItem(
        UUID skuId,
        String productName,
        int quantity,
        BigDecimal unitPrice,
        BigDecimal itemTotalPrice
) {
}
