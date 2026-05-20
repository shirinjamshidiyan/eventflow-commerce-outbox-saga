package com.shirin.order.application;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

public record CreateOrderCommand(
        UUID requestId,
        UUID checkoutId,
        UUID customerId,
        UUID paymentMethodId,
        String currency,
        BigDecimal totalAmount,
        List<CreateOrderCommandItem> items
) {
}
