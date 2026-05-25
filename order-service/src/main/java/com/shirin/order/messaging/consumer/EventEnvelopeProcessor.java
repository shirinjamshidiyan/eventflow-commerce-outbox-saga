package com.shirin.order.messaging.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.shirin.contracts.common.EventEnvelope;
import com.shirin.order.observability.LoggingContext;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import jakarta.validation.Validator;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;

@Slf4j
@Component
@AllArgsConstructor
public class EventEnvelopeProcessor {

    private final ObjectMapper objectMapper;
    private final Validator validator;

    public <T> void process(
            String rawPayload,
            Class<T> payloadType,
            String expectedEventType,
            Function<T, UUID> orderIdExtractor,
            Consumer<EventEnvelope<T>> handler
    ) {

        EventEnvelope<T> envelope = toEnvelope(rawPayload, payloadType);

        validateEnvelope(envelope);
        validateEventType(envelope, expectedEventType);

        UUID orderId = orderIdExtractor.apply(envelope.payload());

        try {
            LoggingContext.GetMDCInfoFromEnvelope(envelope, orderId);

            log.info("Received Kafka event");

            handler.accept(envelope);

            log.info("Kafka event handled");

        } finally {
            LoggingContext.clear();
        }
    }

    private <T> EventEnvelope<T> toEnvelope(String rawPayload, Class<T> payloadType) {
        try {
            JavaType envelopeType = objectMapper
                    .getTypeFactory()
                    .constructParametricType(EventEnvelope.class, payloadType);

            return objectMapper.readValue(rawPayload, envelopeType);

        } catch (JsonProcessingException ex) {
            throw new InvalidEventPayloadException("Invalid event envelope JSON payload", ex);
        }

    }

    private <T> void validateEnvelope(EventEnvelope<T> envelope) {
        Set<ConstraintViolation<EventEnvelope<T>>> violations =
                validator.validate(envelope);

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
