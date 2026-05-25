package com.shirin.payment.messaging.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.shirin.contracts.common.EventEnvelope;
import com.shirin.contracts.common.EventTypes;
import com.shirin.contracts.order.PaymentRequestedPayload;
import com.shirin.payment.application.PaymentApplicationService;
import jakarta.validation.Validator;
import lombok.AllArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;


@Component
@AllArgsConstructor
public class PaymentRequestConsumer {

    private final PaymentApplicationService paymentApplicationService;
    private final EventEnvelopeProcessor envelopeProcessor;

    
    @KafkaListener(
            topics = "${app.kafka.topics.payment-requested}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void consumePaymentRequestedEvent(String payload)
    {
        envelopeProcessor.process(
                payload,
                PaymentRequestedPayload.class,
                EventTypes.PAYMENT_REQUESTED,
                PaymentRequestedPayload::orderId,
                paymentApplicationService::processPaymentRequestedEvent);

    }



}


