package com.shirin.payment.messaging.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.shirin.contracts.order.PaymentRequestedEvent;
import com.shirin.payment.application.PaymentApplicationService;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import jakarta.validation.Validator;
import lombok.AllArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Set;

@Component
@AllArgsConstructor
public class PaymentRequestConsumer {

    private final PaymentApplicationService paymentApplicationService;
    private final ObjectMapper objectMapper;
    private final Validator validator;

    
    @KafkaListener(
            topics = "${app.kafka.topics.payment-requested}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void consumePaymentRequestedEvent(String payload)
    {
        PaymentRequestedEvent event = toEventObject(payload, PaymentRequestedEvent.class);
         validate(event);
        paymentApplicationService.processPaymentRequestedEvent(event);

    }

    private <T> T toEventObject(String payload, Class<T> eventType) {
        try {
            return objectMapper.readValue(payload, eventType);
        } catch (JsonProcessingException ex) {
            throw new InvalidEventPayloadException("Invalid payment event JSON payload", ex);
        }
    }

    private <T> void validate(T event) {
        Set<ConstraintViolation<T>> violations = validator.validate(event);

        if (!violations.isEmpty()) {
            throw new ConstraintViolationException(violations);
        }
    }
}
