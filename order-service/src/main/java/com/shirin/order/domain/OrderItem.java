package com.shirin.order.domain;

import jakarta.persistence.*;
import lombok.Getter;

import java.util.UUID;

@Entity
@Table(name = "order_items")
@Getter
public class OrderItem {
    @Id
    private UUID id;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "order_id", nullable = false)
    private Order order;

    @Column(name = "sku_id", nullable = false)
    private UUID skuId;

    @Column(nullable = false)
    private int quantity;

    protected OrderItem() {}

    OrderItem(UUID id, Order order, UUID skuId, int quantity) {
        this.id = id;
        this.order = order;
        this.skuId = skuId;
        this.quantity = quantity;
    }

}
