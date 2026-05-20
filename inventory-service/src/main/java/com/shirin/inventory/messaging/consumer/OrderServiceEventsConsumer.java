package com.shirin.inventory.messaging.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.shirin.inventory.application.InventoryApplicationService;
import com.shirin.inventory.messaging.events.InventoryReleaseRequestedEvent;
import com.shirin.inventory.messaging.events.OrderCreatedEvent;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import jakarta.validation.Validator;
import lombok.AllArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Set;

@Component
@AllArgsConstructor
public class OrderServiceEventsConsumer {

    private final InventoryApplicationService inventoryService;
    private final ObjectMapper objectMapper;
    private final Validator validator;

    @KafkaListener(
            topics = "${app.kafka.topics.order-created}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void consumeOrderCreatedEvent(String payload) {
        OrderCreatedEvent event = toEventObject(payload, OrderCreatedEvent.class);
        validate(event);
        inventoryService.processOrderCreatedEvent(event);
    }

    @KafkaListener(
            topics = "${app.kafka.topics.inventory-release-requested}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void consumeInventoryReleaseRequestedEvent(String payload)  {
        InventoryReleaseRequestedEvent event = toEventObject(payload, InventoryReleaseRequestedEvent.class);
        validate(event);
        inventoryService.processInventoryReleaseRequestedEvent(event);
    }

    private <T> T toEventObject(String payload,Class<T> eventType ) {
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
