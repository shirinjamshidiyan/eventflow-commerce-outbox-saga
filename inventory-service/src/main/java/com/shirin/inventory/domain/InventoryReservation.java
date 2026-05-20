package com.shirin.inventory.domain;

import jakarta.persistence.*;
import lombok.Getter;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "inventory_reservations")
@Getter
public class InventoryReservation {

    @Id
    private UUID id;

    @Column(name = "order_id", nullable = false)
    private UUID orderId;

    @Column(name = "sku_id", nullable = false)
    private UUID skuId;

    @Column(nullable = false)
    private int quantity;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private InventoryReservationStatus status;

    @Column(name = "created_at" ,nullable = false, insertable = false , updatable = false)
    private Instant createdAt;

    @Column(name = "released_at")
    private Instant releasedAt;

    protected InventoryReservation(){}

     public InventoryReservation(UUID id, UUID orderId, UUID skuId, int quantity) {
            this.id = id;
            this.orderId = orderId;
            this.skuId = skuId;
            this.quantity = quantity;
            this.status = InventoryReservationStatus.RESERVED;
        }

        public void release() {
            if (this.status == InventoryReservationStatus.RELEASED) {
                return;
            }

            this.status = InventoryReservationStatus.RELEASED;
            this.releasedAt = Instant.now();
        }

}
