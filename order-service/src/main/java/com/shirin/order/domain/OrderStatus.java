package com.shirin.order.domain;

public enum OrderStatus {
    INVENTORY_PENDING,
    INVENTORY_RESERVED,
    PAYMENT_PENDING,
    CONFIRMED,
    CANCELLATION_PENDING,
    CANCELLED
}
