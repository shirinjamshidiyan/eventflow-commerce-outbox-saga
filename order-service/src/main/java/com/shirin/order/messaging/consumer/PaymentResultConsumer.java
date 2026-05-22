package com.shirin.order.messaging.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.shirin.contracts.payment.PaymentAuthorizedEvent;
import com.shirin.contracts.payment.PaymentFailedEvent;
import com.shirin.order.application.OrderApplicationService;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import jakarta.validation.Validator;
import lombok.AllArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Set;

@Component
@AllArgsConstructor
public class PaymentResultConsumer {

    private final ObjectMapper objectMapper;
    private final OrderApplicationService orderService;
    private final Validator validator;


    @KafkaListener(
            topics = "${app.kafka.topics.payment-authorized}",
            groupId = "${spring.kafka.consumer.group-id}")
    public void consumePaymentAuthorizedEvent(String payload) {

        PaymentAuthorizedEvent event = toEventObject(payload, PaymentAuthorizedEvent.class);
        validate(event);
        orderService.handlePaymentAuthorizedEvent(event);

    }

    @KafkaListener(
            topics = "${app.kafka.topics.payment-failed}",
            groupId = "${spring.kafka.consumer.group-id}")
    public void consumePaymentFailedEvent(String payload) {

        PaymentFailedEvent event = toEventObject(payload, PaymentFailedEvent.class);
        validate(event);
        orderService.handlePaymentFailedEvent(event);

    }
    
    private <T> T toEventObject(String payload, Class<T> eventType) {
        try {
            return objectMapper.readValue(payload, eventType);
        } catch (JsonProcessingException ex) {
            throw new InvalidEventPayloadException("Invalid payment result event JSON payload", ex);
        }
    }
    private <T> void validate(T event) {
        Set<ConstraintViolation<T>> violations = validator.validate(event);

        if (!violations.isEmpty()) {
            throw new ConstraintViolationException(violations);
        }
    }
}
