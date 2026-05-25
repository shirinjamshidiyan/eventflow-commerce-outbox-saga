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
    private final EventEnvelopeProcessor envelopeProcessor;

    @KafkaListener(
            topics = "${app.kafka.topics.order-created}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void consumeOrderCreatedEvent(String payload) {
        envelopeProcessor.process(
                payload,
                OrderCreatedPayload.class,
                EventTypes.ORDER_CREATED,
                OrderCreatedPayload::orderId,
                inventoryService::processOrderCreatedEvent
        );

    }

    @KafkaListener(
            topics = "${app.kafka.topics.inventory-release-requested}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void consumeInventoryReleaseRequestedEvent(String payload)  {
        envelopeProcessor.process(
                payload,
                InventoryReleaseRequestedPayload.class,
                EventTypes.INVENTORY_RELEASE_REQUESTED,
                InventoryReleaseRequestedPayload::orderId,
                inventoryService::processInventoryReleaseRequestedEvent
        );
        
    }






}
