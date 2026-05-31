package com.shirin.contracts.common;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Instant;
import java.util.UUID;

/*
Contains metadata for tracking, tracing, debugging, replay and observability.
 */
public record EventEnvelope<T> (
        @NotNull UUID eventId,
        @NotBlank String eventType,
        @Min(1) int eventVersion,
        @NotNull UUID correlationId,
        UUID causationId,
        @NotNull Instant occurredAt,
        @NotBlank String source,
        @NotNull @Valid T payload
){

    public static <T> EventEnvelope<T> create(
            UUID eventId,
            String eventType,
            int eventVersion,
            UUID correlationId,
            UUID causationId,
            String source,
            T payload
    ) {
        return new EventEnvelope<>(
                eventId,
                eventType,
                eventVersion,
                correlationId,
                causationId,
                Instant.now(),
                source,
                payload
        );
    }
}
