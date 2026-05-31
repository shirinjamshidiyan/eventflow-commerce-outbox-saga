package com.shirin.order.domain;

import jakarta.persistence.*;
import lombok.Getter;
import java.math.BigDecimal;
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

    @Column(name = "product_name", nullable = false)
    private String productName;

    @Column(nullable = false)
    private int quantity;

    @Column(name = "unit_price", nullable = false, precision = 12, scale = 2)
    private BigDecimal unitPrice;

    @Column(name = "item_total_price", nullable = false, precision = 12 , scale = 2)
    private BigDecimal itemTotalPrice;

    protected OrderItem() {}

    OrderItem(
            UUID id,
            Order order,
            UUID skuId,
            String name,
            int quantity,
            BigDecimal unitPrice,
            BigDecimal itemTotalPrice
    ) {
        this.id = id;
        this.order = order;
        this.skuId = skuId;
        this.productName = name;
        this.quantity = quantity;
        this.unitPrice = unitPrice;
        this.itemTotalPrice = itemTotalPrice;
    }

}
