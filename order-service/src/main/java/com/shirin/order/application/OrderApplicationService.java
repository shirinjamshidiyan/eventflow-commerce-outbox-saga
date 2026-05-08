package com.shirin.order.application;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.shirin.order.domain.Order;
import com.shirin.order.domain.OrderRepository;
import com.shirin.order.messaging.events.OrderCreatedEvent;
import com.shirin.order.messaging.events.OrderCreatedEventItem;
import com.shirin.order.outbox.OutboxEvent;
import com.shirin.order.outbox.OutboxEventRepository;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.util.List;
import java.util.UUID;

@Service
@AllArgsConstructor
public class OrderApplicationService {

    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    @Transactional
    public UUID createOrder(CreateOrderCommand command) throws JsonProcessingException {

        UUID orderId = UUID.randomUUID();
        Order order = new Order(orderId);
        command.items()
                .forEach(item -> order.addItem(item.skuId(), item.quantity()));

        orderRepository.save(order);

        UUID eventId = UUID.randomUUID();

        List<OrderCreatedEventItem> eventItems = order.getItems()
                .stream()
                .map(item ->
                        new OrderCreatedEventItem(
                                item.getSkuId(),
                                item.getQuantity())
                )
                .toList();

        OrderCreatedEvent event = new OrderCreatedEvent(eventId, orderId, eventItems);

        OutboxEvent outboxEvent = OutboxEvent.pending(
                eventId,
                "Order",
                orderId,
                "OrderCreated",
                objectMapper.writeValueAsString(event));

        outboxEventRepository.save(outboxEvent);

        return orderId;
    }

}


