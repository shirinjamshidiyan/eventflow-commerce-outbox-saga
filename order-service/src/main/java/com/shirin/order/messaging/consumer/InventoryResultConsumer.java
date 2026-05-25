package com.shirin.order.messaging.consumer;

import com.shirin.contracts.common.EventTypes;
import com.shirin.contracts.inventory.InventoryReleasedPayload;
import com.shirin.contracts.inventory.InventoryReservationFailedPayload;
import com.shirin.contracts.inventory.InventoryReservedPayload;
import com.shirin.order.application.OrderApplicationService;
import lombok.AllArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@AllArgsConstructor
public class InventoryResultConsumer {

    private final EventEnvelopeProcessor envelopeProcessor;
    private final OrderApplicationService orderService;

    @KafkaListener(
            topics = "${app.kafka.topics.inventory-reserved}",
            groupId = "${spring.kafka.consumer.group-id}")
    public void consumeInventoryReservedEvent(String payload) {

        envelopeProcessor.process(
                payload,
                InventoryReservedPayload.class,
                EventTypes.INVENTORY_RESERVED,
                InventoryReservedPayload::orderId,
                orderService::handleInventoryReservedEvent
        );

    }

    @KafkaListener(
            topics = "${app.kafka.topics.inventory-reservation-failed}",
            groupId = "${spring.kafka.consumer.group-id}")
    public void consumeInventoryReservationFailedEvent(String payload) {
        envelopeProcessor.process(
                payload,
                InventoryReservationFailedPayload.class,
                EventTypes.INVENTORY_RESERVATION_FAILED,
                InventoryReservationFailedPayload::orderId,
                orderService::handleInventoryReservationFailedEvent
        );

    }

    @KafkaListener(
            topics = "${app.kafka.topics.inventory-released}",
            groupId = "${spring.kafka.consumer.group-id}")
    public void consumeInventoryReleasedEvent(String payload) {

        envelopeProcessor.process(
                payload,
                InventoryReleasedPayload.class,
                EventTypes.INVENTORY_RELEASED,
                InventoryReleasedPayload::orderId,
                orderService::handleInventoryReleasedEvent
        );

    }
}
