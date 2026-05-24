package com.shirin.payment.application;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.shirin.contracts.common.EventEnvelope;
import com.shirin.contracts.common.EventSources;
import com.shirin.contracts.common.EventTypes;
import com.shirin.contracts.order.PaymentRequestedPayload;
import com.shirin.contracts.payment.PaymentAuthorizedPayload;
import com.shirin.contracts.payment.PaymentFailedPayload;
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
    public void processPaymentRequestedEvent(EventEnvelope<PaymentRequestedPayload> envelope)
    {

        // idempotency check1: prevents processing the same event
        int inserted  = idempotencyRepository.insertIfAbsent(envelope.eventId());
        if(inserted ==0) return;

        // Idempotency check 2: prevents creating more than one payment for the same order.
        Payment existingPayment = paymentRepository
                .findByOrderId(envelope.payload().orderId())
                .orElse(null);

        if (existingPayment != null) {
            return;
        }
        PaymentRequestedPayload payload = envelope.payload();

        Payment payment = new Payment(
                UUID.randomUUID(),
                payload.orderId(),
                payload.paymentMethodId(),
                payload.currency(),
                payload.amount()
        );

        PaymentDecision decision = authorizer.authorize( payload.paymentMethodId(), payload.amount()); //Simulation

        if (decision.approved()) {

            payment.authorize();
            paymentRepository.save(payment);

            UUID eventId= UUID.randomUUID();
            PaymentAuthorizedPayload newPayload = new PaymentAuthorizedPayload(
                    payload.orderId(),
                    payment.getId()
            );
            EventEnvelope<PaymentAuthorizedPayload> newEnvelope = EventEnvelope.create(
                    eventId,
                    EventTypes.PAYMENT_AUTHORIZED,
                    1,
                    envelope.correlationId(),
                    envelope.eventId(),
                    EventSources.PAYMENT_SERVICE,
                    newPayload);

            outboxRepository.save(OutboxEvent.createPendingEvent(
                    eventId,
                    "Payment",
                    payload.orderId(),
                    EventTypes.PAYMENT_AUTHORIZED,
                    toJson(newEnvelope)
            ));
            return;

        }

        payment.fail(decision.reason());
        paymentRepository.save(payment);

        UUID eventId= UUID.randomUUID();

        PaymentFailedPayload failedPayload = new PaymentFailedPayload(
                payload.orderId(),
                payment.getId(),
                decision.reason()
        );
        EventEnvelope<PaymentFailedPayload> newEnvelope = EventEnvelope.create(
                eventId,
                EventTypes.PAYMENT_FAILED,
                1,
                envelope.correlationId(),
                envelope.eventId(),
                EventSources.PAYMENT_SERVICE,
                failedPayload);

        outboxRepository.save(OutboxEvent.createPendingEvent(
                eventId,
                "Payment",
                payload.orderId(),
                EventTypes.PAYMENT_FAILED,
                toJson(newEnvelope)
        ));

    }


    private String toJson(Object envelope) {
        try {
            return objectMapper.writeValueAsString(envelope);
        } catch (JsonProcessingException ex) {
            throw new EventSerializationException("Failed to serialize outgoing payment envelope", ex);
        }
    }
}
