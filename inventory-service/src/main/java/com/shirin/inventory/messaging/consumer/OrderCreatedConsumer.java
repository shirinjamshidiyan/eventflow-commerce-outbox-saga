package com.shirin.inventory.messaging.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.shirin.inventory.application.InventoryApplicationService;
import com.shirin.inventory.messaging.events.OrderCreatedEvent;
import lombok.AllArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@AllArgsConstructor
public class OrderCreatedConsumer {

    private final InventoryApplicationService inventoryService;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = "${app.kafka.topics.order-created}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void consumeOrderCreatedEvent(String payload) throws JsonProcessingException {
        OrderCreatedEvent event =
                objectMapper.readValue(payload, OrderCreatedEvent.class);

        inventoryService.ProcessOrderCreatedEvent(event);
    }
}
