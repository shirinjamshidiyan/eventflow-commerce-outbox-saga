package com.shirin.inventory.messaging.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.shirin.inventory.application.InventoryApplicationService;
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
public class OrderCreatedConsumer {

    private final InventoryApplicationService inventoryService;
    private final ObjectMapper objectMapper;
    private final Validator validator;

    @KafkaListener(
            topics = "${app.kafka.topics.order-created}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void consumeOrderCreatedEvent(String payload) {
        OrderCreatedEvent event = toEvent(payload);
        validate(event);
        inventoryService.processOrderCreatedEvent(event);
    }


    private OrderCreatedEvent toEvent(String payload) {
        try {
            return objectMapper.readValue(payload, OrderCreatedEvent.class);
        } catch (JsonProcessingException ex) {
            throw new InvalidEventPayloadException("Invalid OrderCreated JSON payload", ex);
        }
    }
    private void validate(OrderCreatedEvent event) {
        Set<ConstraintViolation<OrderCreatedEvent>> violations =
                validator.validate(event);

        if (!violations.isEmpty()) {
            throw new ConstraintViolationException(violations);
        }
    }
}
