package com.shirin.order.observability;

import com.shirin.contracts.common.EventEnvelope;
import org.slf4j.MDC;
import java.util.UUID;

public final class LoggingContext {

    private LoggingContext(){}


    public static void getMDCInfoFromEnvelope(EventEnvelope<?> envelope, UUID orderId) {
        if(envelope.correlationId()!= null)
            MDC.put("correlationId", envelope.correlationId().toString());

        if(envelope.eventId()!= null)
            MDC.put("eventId", envelope.eventId().toString());

        if(envelope.eventType()!= null)
            MDC.put("eventType", envelope.eventType());

        if(orderId != null)
            MDC.put("orderId", orderId.toString());

        if(envelope.causationId()!= null)
            MDC.put("causationId", envelope.causationId().toString());

        if(envelope.source() != null)
            MDC.put("source", envelope.source());

    }

    public static void putRequestId(UUID requestId) {
        if(requestId!= null)
            MDC.put("requestId", requestId.toString());
    }

    public static void putOrderId(UUID orderId) {
        if(orderId != null )
            MDC.put("orderId", orderId.toString());
    }

    public static UUID currentCorrelationId() {
        String value = MDC.get("correlationId");

        if (value == null || value.isBlank()) {
            UUID generated = UUID.randomUUID();
            MDC.put("correlationId", generated.toString());
            return generated;
        }
        return UUID.fromString(value);
    }

    public static void clear() {
        MDC.clear();
    }
}