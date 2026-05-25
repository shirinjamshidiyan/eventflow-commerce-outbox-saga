package com.shirin.order.messaging.consumer;

import com.shirin.contracts.common.EventEnvelope;
import com.shirin.contracts.common.EventTypes;
import com.shirin.contracts.payment.PaymentAuthorizedPayload;
import com.shirin.contracts.payment.PaymentFailedPayload;
import com.shirin.order.application.OrderApplicationService;
import com.shirin.order.observability.LoggingContext;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@AllArgsConstructor
@Slf4j
public class PaymentResultConsumer {

    private final OrderApplicationService orderService;
    private final EventEnvelopeProcessor envelopeProcessor;


    @KafkaListener(
            topics = "${app.kafka.topics.payment-authorized}",
            groupId = "${spring.kafka.consumer.group-id}")
    public void consumePaymentAuthorizedEvent(String payload) {

        envelopeProcessor.process(
                payload,
                PaymentAuthorizedPayload.class,
                EventTypes.PAYMENT_AUTHORIZED,
                PaymentAuthorizedPayload::orderId,
                orderService::handlePaymentAuthorizedEvent

        );

    }

    @KafkaListener(
            topics = "${app.kafka.topics.payment-failed}",
            groupId = "${spring.kafka.consumer.group-id}")
    public void consumePaymentFailedEvent(String payload) {

        envelopeProcessor.process(
                payload,
                PaymentFailedPayload.class,
                EventTypes.PAYMENT_FAILED,
                PaymentFailedPayload::orderId,
                orderService::handlePaymentFailedEvent
        );

    }

}
