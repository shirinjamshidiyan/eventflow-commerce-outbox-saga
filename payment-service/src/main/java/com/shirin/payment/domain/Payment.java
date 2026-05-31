package com.shirin.payment.domain;

import jakarta.persistence.*;
import lombok.Getter;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name="payments")
@Getter
public class Payment {

    @Id
    private UUID id;
    @Column(name = "order_id", unique = true, nullable = false)
    private UUID orderId;

    @Column(name = "payment_method_id", nullable = false)
    private UUID paymentMethodId;

    @Column(nullable = false, precision = 12, scale = 2)
    private BigDecimal amount;

    @Column(nullable = false, length = 3)
    private String currency;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private PaymentStatus status;

    @Column(name = "failure_reason", columnDefinition = "TEXT")
    private String failureReason;

    @Column(name = "created_at", insertable = false, updatable = false, nullable = false)
    private Instant createdAt;

    @Column(name = "authorized_at")
    private Instant authorizedAt;

    @Column(name = "failed_at")
    private Instant failedAt;

    protected Payment() {}

    public Payment(
            UUID id,
            UUID orderId,
            UUID paymentMethodId,
            String currency,
            BigDecimal amount
    ) {
        this.id = id;
        this.orderId = orderId;
        this.paymentMethodId = paymentMethodId;
        this.currency = currency;
        this.amount = amount;
        this.status = PaymentStatus.PENDING;
    }

    public void authorize() {
        if (this.status != PaymentStatus.PENDING) {
            return;
        }

        this.status = PaymentStatus.AUTHORIZED;
        this.authorizedAt = Instant.now();
    }

    public void fail(String reason) {
        if (this.status != PaymentStatus.PENDING) {
            return;
        }

        this.status = PaymentStatus.FAILED;
        this.failureReason = reason;
        this.failedAt = Instant.now();
    }




}
