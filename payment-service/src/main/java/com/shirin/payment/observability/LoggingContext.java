package com.shirin.payment.observability;

import com.shirin.contracts.common.EventEnvelope;
import org.slf4j.MDC;

import java.util.UUID;

public final class LoggingContext {

    private LoggingContext(){}

    public static void GetMDCInfoFromEnvelope(EventEnvelope<?> envelope, UUID orderId) {
        if(envelope.correlationId()!= null)
            MDC.put("correlationId", envelope.correlationId().toString());

        if(envelope.eventId()!= null)
            MDC.put("eventId", envelope.eventId().toString());

        if(envelope.eventType()!= null)
            MDC.put("eventType", envelope.eventType());

        if(orderId != null)
            MDC.put("orderId", orderId.toString());

    }

    public static void clear() {
        MDC.clear();
    }

}