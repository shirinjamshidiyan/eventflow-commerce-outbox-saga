package com.shirin.order.messaging.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.shirin.order.application.OrderApplicationService;
import com.shirin.order.messaging.events.InventoryReservationFailedEvent;
import com.shirin.order.messaging.events.InventoryReservedEvent;
import lombok.AllArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@AllArgsConstructor
public class EventoryEventsConsumer {

    private final ObjectMapper objectMapper;
    private final OrderApplicationService service;

    @KafkaListener(
            topics = "${app.kafka.topics.inventory-reserved}",
            groupId = "${spring.kafka.consumer.group-id}")
    public void consumeInventoryReservedEvent(String payload) throws JsonProcessingException {
        InventoryReservedEvent event =
                objectMapper.readValue(payload, InventoryReservedEvent.class);

        service.handleInventoryReservedEvent(event);

    }

    @KafkaListener(
            topics = "${app.kafka.topics.inventory-reservation-failed}",
            groupId = "${spring.kafka.consumer.group-id}")
    public void consumeInventoryReservationFailedEvent(String payload) throws JsonProcessingException {

        InventoryReservationFailedEvent event =
                objectMapper.readValue(payload, InventoryReservationFailedEvent.class);

        service.handleInventoryReservationFailedEvent(event);

    }
}
