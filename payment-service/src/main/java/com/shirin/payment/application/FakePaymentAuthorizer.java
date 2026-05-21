package com.shirin.payment.application;

import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.UUID;

/*
Fake Payment condition:
    paymentMethodId starts with 22222222 -> business failure
    paymentMethodId starts with 33333333 -> technical failure
    amount > 1000.00 -> business failure
    else -> success

 */
@Component
public class FakePaymentAuthorizer{

    public PaymentDecision authorize(UUID paymentMethodId, BigDecimal amount) {
        String value = paymentMethodId.toString();

        if (value.startsWith("22222222")) {
            return new PaymentDecision(false,"Card declined" );
        }

        if (value.startsWith("33333333")) {
            throw new IllegalStateException("Simulated payment provider error");
        }
        if (amount.compareTo(new BigDecimal("1000.00")) > 0) {
            return new PaymentDecision(false, "Amount exceeds test authorization limit");
        }

        return new PaymentDecision(true, null);
    }
}
