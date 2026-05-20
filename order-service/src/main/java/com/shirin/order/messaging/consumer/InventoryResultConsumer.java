package com.shirin.order.messaging.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.shirin.order.application.OrderApplicationService;
import com.shirin.order.messaging.events.InventoryReleasedEvent;
import com.shirin.order.messaging.events.InventoryReservationFailedEvent;
import com.shirin.order.messaging.events.InventoryReservedEvent;
import com.shirin.order.messaging.events.OrderCreatedEvent;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import jakarta.validation.Validator;
import lombok.AllArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Set;

@Component
@AllArgsConstructor
public class InventoryResultConsumer {

    private final ObjectMapper objectMapper;
    private final OrderApplicationService orderService;
    private final Validator validator;

    @KafkaListener(
            topics = "${app.kafka.topics.inventory-reserved}",
            groupId = "${spring.kafka.consumer.group-id}")
    public void consumeInventoryReservedEvent(String payload) {

        InventoryReservedEvent event = toEventObject(payload, InventoryReservedEvent.class);
        validate(event);
        orderService.handleInventoryReservedEvent(event);

    }

    @KafkaListener(
            topics = "${app.kafka.topics.inventory-reservation-failed}",
            groupId = "${spring.kafka.consumer.group-id}")
    public void consumeInventoryReservationFailedEvent(String payload) {

        InventoryReservationFailedEvent event = toEventObject(payload, InventoryReservationFailedEvent.class);
        validate(event);
        orderService.handleInventoryReservationFailedEvent(event);

    }

    @KafkaListener(
            topics = "${app.kafka.topics.inventory-released}",
            groupId = "${spring.kafka.consumer.group-id}")
    public void consumeInventoryReleasedEvent(String payload) {

        InventoryReleasedEvent event = toEventObject(payload, InventoryReleasedEvent.class);
        validate(event);
        orderService.handleInventoryReleasedEvent(event);

    }


    private <T> T toEventObject(String payload, Class<T> eventType) {
        try {
            return objectMapper.readValue(payload, eventType);
        } catch (JsonProcessingException ex) {
            throw new InvalidEventPayloadException("Invalid event JSON payload", ex);
        }
    }
    private <T> void validate(T event) {
        Set<ConstraintViolation<T>> violations = validator.validate(event);

        if (!violations.isEmpty()) {
            throw new ConstraintViolationException(violations);
        }
    }
}
