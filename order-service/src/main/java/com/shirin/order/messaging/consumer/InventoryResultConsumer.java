package com.shirin.order.messaging.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.shirin.contracts.common.EventEnvelope;
import com.shirin.contracts.common.EventTypes;
import com.shirin.contracts.inventory.InventoryReleasedPayload;
import com.shirin.contracts.inventory.InventoryReservationFailedPayload;
import com.shirin.contracts.inventory.InventoryReservedPayload;
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
public class InventoryResultConsumer {

    private final ObjectMapper objectMapper;
    private final OrderApplicationService orderService;
    private final Validator validator;

    @KafkaListener(
            topics = "${app.kafka.topics.inventory-reserved}",
            groupId = "${spring.kafka.consumer.group-id}")
    public void consumeInventoryReservedEvent(String payload) {

        EventEnvelope<InventoryReservedPayload> envelope = toEnvelope(payload, InventoryReservedPayload.class);

        validateEnvelope(envelope);
        validateEventType(envelope, EventTypes.INVENTORY_RESERVED);

        orderService.handleInventoryReservedEvent(envelope);

    }

    @KafkaListener(
            topics = "${app.kafka.topics.inventory-reservation-failed}",
            groupId = "${spring.kafka.consumer.group-id}")
    public void consumeInventoryReservationFailedEvent(String payload) {

        EventEnvelope<InventoryReservationFailedPayload> envelope = toEnvelope (payload,InventoryReservationFailedPayload.class);

        validateEnvelope(envelope);
        validateEventType(envelope, EventTypes.INVENTORY_RESERVATION_FAILED);

        orderService.handleInventoryReservationFailedEvent(envelope);

    }

    @KafkaListener(
            topics = "${app.kafka.topics.inventory-released}",
            groupId = "${spring.kafka.consumer.group-id}")
    public void consumeInventoryReleasedEvent(String payload) {

        EventEnvelope<InventoryReleasedPayload> envelope = toEnvelope(payload,InventoryReleasedPayload.class);

        validateEnvelope(envelope);
        validateEventType(envelope, EventTypes.INVENTORY_RELEASED);

        orderService.handleInventoryReleasedEvent(envelope);

    }


    private <T> EventEnvelope<T> toEnvelope(String payload, Class<T> payloadType) {
        try {
            JavaType envelopeType = objectMapper
                    .getTypeFactory()
                    .constructParametricType(EventEnvelope.class, payloadType);

            return objectMapper.readValue(payload , envelopeType);
        } catch (JsonProcessingException ex) {
            throw new InvalidEventPayloadException("Invalid inventory result envelope JSON payload", ex);
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

    private <T> void validateEnvelope(EventEnvelope<T> envelope) {
        Set<ConstraintViolation<EventEnvelope<T>>> violations = validator.validate(envelope);

        if (!violations.isEmpty()) {
            throw new ConstraintViolationException(violations);
        }
    }
}
