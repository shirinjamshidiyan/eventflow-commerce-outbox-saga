package com.shirin.payment.application;
public record PaymentDecision(
        boolean approved,
        String reason
) {
}
