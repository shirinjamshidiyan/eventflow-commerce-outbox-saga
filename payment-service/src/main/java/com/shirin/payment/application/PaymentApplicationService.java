package com.shirin.payment.application;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.shirin.contracts.order.PaymentRequestedEvent;
import com.shirin.contracts.payment.PaymentAuthorizedEvent;
import com.shirin.contracts.payment.PaymentFailedEvent;
import com.shirin.payment.domain.Payment;
import com.shirin.payment.domain.PaymentRepository;
import com.shirin.payment.idempotency.ProcessedEventsRepository;
import com.shirin.payment.outbox.OutboxEvent;
import com.shirin.payment.outbox.OutboxEventRepository;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
@AllArgsConstructor
public class PaymentApplicationService {

    private final ProcessedEventsRepository idempotencyRepository;
    private final PaymentRepository paymentRepository;
    private final FakePaymentAuthorizer authorizer;
    private final OutboxEventRepository outboxRepository;
    private final ObjectMapper objectMapper;

    @Transactional
    public void processPaymentRequestedEvent(PaymentRequestedEvent event)
    {

        //idempotency check1: prevents processing the same event
        int exist = idempotencyRepository.insertIfAbsent(event.eventId());
        if(exist==0) return;

        // //idempotency check2: Prevents creating multiple payments for the same order.
        Payment existingPayment = paymentRepository
                .findByOrderId(event.orderId())
                .orElse(null);

        if (existingPayment != null) {
            return;
        }

        Payment payment = new Payment(
                UUID.randomUUID(),
                event.orderId(),
                event.paymentMethodId(),
                event.currency(),
                event.amount()
        );

        PaymentDecision decision = authorizer.authorize( event.paymentMethodId(), event.amount()); //Simulation

        if (decision.approved()) {

            payment.authorize();
            paymentRepository.save(payment);

            PaymentAuthorizedEvent resultEvent = new PaymentAuthorizedEvent(
                    UUID.randomUUID(),
                    event.orderId(),
                    payment.getId()
            );

            outboxRepository.save(OutboxEvent.createPendingEvent(
                    resultEvent.eventId(),
                    "Payment",
                    payment.getId(),
                    "PaymentAuthorized",
                    toJson(resultEvent)
            ));
            return;

        }
        payment.fail(decision.reason());
        paymentRepository.save(payment);

        PaymentFailedEvent resultEvent = new PaymentFailedEvent(
                UUID.randomUUID(),
                event.orderId(),
                payment.getId(),
                decision.reason()
        );
        outboxRepository.save(OutboxEvent.createPendingEvent(
                resultEvent.eventId(),
                "Payment",
                payment.getId(),
                "PaymentFailed",
                toJson(resultEvent)
        ));
    }


    private String toJson(Object event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException ex) {
            throw new EventSerializationException("Failed to serialize outgoing payment event", ex);
        }
    }
}
