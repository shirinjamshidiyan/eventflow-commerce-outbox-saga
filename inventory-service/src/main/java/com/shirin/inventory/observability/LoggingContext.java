package com.shirin.inventory.observability;

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

    public static void clear() {
        MDC.clear();
    }

}