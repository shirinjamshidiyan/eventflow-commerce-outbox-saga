package com.shirin.order.domain;

import jakarta.persistence.*;
import lombok.Getter;

import java.math.BigDecimal;
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

    @Column(name = "order_number", nullable = false, unique = true)
    private String orderNumber;

    @Column(name = "request_id", nullable = false, unique = true)
    private UUID requestId;

    @Column(name = "checkout_id", nullable = false)
    private UUID checkoutId;

    @Column(name = "customer_id", nullable = false)
    private UUID customerId;

    @Column(name = "payment_method_id", nullable = false)
    private UUID paymentMethodId;

    @Column(name = "currency", nullable = false, length = 3)
    private String currency;

    @Column(name = "total_amount", nullable = false, precision = 12, scale = 2)
    private BigDecimal totalAmount;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private OrderStatus status;

    @Column(name = "cancellation_reason", columnDefinition = "TEXT")
    private String cancellationReason;

    @Column(name = "created_at", insertable = false, updatable = false, nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at")
    private Instant updatedAt;

    @Column(name = "confirmed_at")
    private Instant confirmedAt;

    @Column(name = "cancelled_at")
    private Instant cancelledAt;

    @Version
    @Column(name = "version", nullable = false)
    private Long version;

    @OneToMany(mappedBy = "order", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<OrderItem> items = new ArrayList<>();

    protected Order() {}

    //    public void changeStatusToCancelled(String reason) {
//        if (this.status == OrderStatus.INVENTORY_RESERVED) {
//            return;
//        }
//        this.status = OrderStatus.CANCELLED;
//    }
    //////////////////////////
   public Order(
                UUID id,
                String orderNumber,
                UUID requestId,
                UUID checkoutId,
                UUID customerId,
                UUID paymentMethodId,
                String currency,
                BigDecimal totalAmount
        ) {
            this.id = id;
            this.orderNumber = orderNumber;
            this.requestId = requestId;
            this.checkoutId = checkoutId;
            this.customerId = customerId;
            this.paymentMethodId = paymentMethodId;
            this.currency = currency;
            this.totalAmount = totalAmount;
            this.status = OrderStatus.INVENTORY_PENDING;
        }

     public void addItem(
                UUID skuId,
                String productName,
                int quantity,
                BigDecimal unitPrice,
                BigDecimal itemTotalPrice
        ) {
            OrderItem item = new OrderItem(
                    UUID.randomUUID(),
                    this,
                    skuId,
                    productName,
                    quantity,
                    unitPrice,
                    itemTotalPrice
            );
            this.items.add(item);
        }

//        public void changeStatusToInventoryReserved() {
//        if (this.status == OrderStatus.CANCELLED) {
//            return;
//        }
//        if(this.status == OrderStatus.INVENTORY_PENDING) {
//            this.status = OrderStatus.INVENTORY_RESERVED;
//            touch();
//        }
//    }

    @PrePersist
    void onCreate() {
        this.updatedAt = Instant.now();
    }

    @PreUpdate
    void onUpdate() {
        this.updatedAt = Instant.now();
    }

    //
    //    public void markPaymentPending() {
    //        if (this.status == OrderStatus.INVENTORY_RESERVED) {
    //            this.status = OrderStatus.PAYMENT_PENDING;
    //            touch();
    //        }
    //    }
    //
    //    public void confirm() {
    //        if (this.status == OrderStatus.PAYMENT_PENDING) {
    //            this.status = OrderStatus.CONFIRMED;
    //            this.confirmedAt = Instant.now();
    //            touch();
    //        }
    //    }
//public void markCancellationPending(String reason) {
//        if (this.status == OrderStatus.INVENTORY_RESERVED
//                || this.status == OrderStatus.PAYMENT_PENDING) {
//            this.status = OrderStatus.CANCELLATION_PENDING;
//            this.cancellationReason = reason;
//            touch();
//        }
//    }
//
//    public void cancel(String reason) {
//        if (this.status != OrderStatus.CONFIRMED) {
//            this.status = OrderStatus.CANCELLED;
//            this.cancellationReason = reason;
//            this.cancelledAt = Instant.now();
//            touch();
//        }
//    }
    // private void touch() {
    //        this.updatedAt = Instant.now();
    //    }




}
