package com.shirin.order.messaging.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.shirin.order.application.OrderApplicationService;
import com.shirin.order.messaging.events.InventoryReservationFailedEvent;
import com.shirin.order.messaging.events.InventoryReservedEvent;
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
    private final OrderApplicationService service;
    private final Validator validator;


    
    private <T> T toEvent(String payload, Class<T> eventType) {
        try {
            return objectMapper.readValue(payload, eventType);
        } catch (JsonProcessingException ex) {
            throw new InvalidEventPayloadException("Invalid JSON payload", ex);
        }
    }
    private <T> void validate(T event) {
        Set<ConstraintViolation<T>> violations = validator.validate(event);

        if (!violations.isEmpty()) {
            throw new ConstraintViolationException(violations);
        }
    }
}
