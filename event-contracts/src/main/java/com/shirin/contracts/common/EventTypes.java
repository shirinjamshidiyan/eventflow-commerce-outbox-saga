package com.shirin.contracts.common;

public final class EventTypes {
    private EventTypes(){}
    public static final String ORDER_CREATED = "OrderCreated";
    public static final String INVENTORY_RESERVED = "InventoryReserved";
    public static final String INVENTORY_RESERVATION_FAILED = "InventoryReservationFailed";
    public static final String INVENTORY_RELEASE_REQUESTED = "InventoryReleaseRequested";
    public static final String INVENTORY_RELEASED = "InventoryReleased";
    public static final String PAYMENT_REQUESTED = "PaymentRequested";
    public static final String PAYMENT_AUTHORIZED = "PaymentAuthorized";
    public static final String PAYMENT_FAILED = "PaymentFailed";
}
