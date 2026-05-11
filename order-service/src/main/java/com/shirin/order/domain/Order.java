package com.shirin.order.domain;

import jakarta.persistence.*;
import lombok.Getter;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Entity
@Table(name = "orders")
@Getter
public class Order {

    @Id
    private UUID id;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private OrderStatus status;

    @Column(name = "created_at", insertable = false, nullable = false)
    private Instant createdAt;

    @OneToMany(mappedBy = "order", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<OrderItem> items = new ArrayList<>();

    protected Order() {}

    public Order(UUID id) {
        this.id = id;
        this.status = OrderStatus.PENDING_INVENTORY;
    }

    public void addItem(UUID skuId, int quantity) {
        OrderItem item = new OrderItem(UUID.randomUUID(),this, skuId, quantity);
        if(this.items == null)
            this.items = new ArrayList<>();

        this.items.add(item);
    }

}
