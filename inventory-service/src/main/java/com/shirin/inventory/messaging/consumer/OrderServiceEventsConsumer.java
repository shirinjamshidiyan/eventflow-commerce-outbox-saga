package com.shirin.inventory.messaging.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.shirin.contracts.common.EventEnvelope;
import com.shirin.contracts.common.EventTypes;
import com.shirin.contracts.order.InventoryReleaseRequestedPayload;
import com.shirin.contracts.order.OrderCreatedPayload;
import com.shirin.inventory.application.InventoryApplicationService;
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
        EventEnvelope<OrderCreatedPayload> envelope = toEnvelope(payload,OrderCreatedPayload.class);

        validateEnvelope(envelope);
        validateEventType(envelope, EventTypes.ORDER_CREATED);

        inventoryService.processOrderCreatedEvent(envelope);
    }

    @KafkaListener(
            topics = "${app.kafka.topics.inventory-release-requested}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void consumeInventoryReleaseRequestedEvent(String payload)  {

        EventEnvelope<InventoryReleaseRequestedPayload> envelope = toEnvelope(payload, InventoryReleaseRequestedPayload.class);

        validateEnvelope(envelope);
        validateEventType(envelope, EventTypes.INVENTORY_RELEASE_REQUESTED);

        inventoryService.processInventoryReleaseRequestedEvent(envelope);
    }

    private <T> EventEnvelope<T> toEnvelope(String payload,Class<T> payloadType ) {
        try {
            JavaType envelopeType = objectMapper
                    .getTypeFactory()
                    .constructParametricType(EventEnvelope.class, payloadType);

            return objectMapper.readValue(payload, envelopeType);
        } catch (JsonProcessingException ex) {
            throw new InvalidEventPayloadException("Invalid Inventory envelope JSON payload", ex);
        }
    }

    private <T> void validateEnvelope(EventEnvelope<T> envelope) {
        Set<ConstraintViolation<EventEnvelope<T>>> violations = validator.validate(envelope);

        if (!violations.isEmpty()) {
            throw new ConstraintViolationException(violations);
        }
    }
    private void validateEventType(EventEnvelope<?> envelope, String expectedEventType) {
        if (!expectedEventType.equals(envelope.eventType())) {
            throw new InvalidEventPayloadException(
                    "Unexpected event type. Expected: "
                            + expectedEventType
                            + ", actual: "
                            + envelope.eventType()
            );
        }
    }


}
