package com.shirin.order.application;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.shirin.contracts.order.OrderCreatedEvent;
import com.shirin.contracts.order.OrderCreatedEventItem;
import com.shirin.order.domain.Order;
import com.shirin.order.domain.OrderRepository;
import com.shirin.order.outbox.OutboxEvent;
import com.shirin.order.outbox.OutboxEventRepository;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

@Service
@AllArgsConstructor
public class OrderCreationTxService  {

    private final OrderRepository orderRepository;
    private final OrderNumberGenerator orderNumberGenerator;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;
    @Transactional
    public CreateOrderResult createNewOrder(CreateOrderCommand command) {

        validateCheckoutSnapshot(command);

        UUID orderId = UUID.randomUUID();
        Order order = new Order(
                orderId,
                orderNumberGenerator.generate(),
                command.requestId(),
                command.checkoutId(),
                command.customerId(),
                command.paymentMethodId(),
                command.currency(),
                command.totalAmount()
        );
        command.items().forEach(
                item -> order.addItem(
                        item.skuId(),
                        item.productName(),
                        item.quantity(),
                        item.unitPrice(),
                        item.itemTotalPrice()
                )
        );

        orderRepository.saveAndFlush(order);

        UUID eventId = UUID.randomUUID();

        List<OrderCreatedEventItem> eventItems = order.getItems()
                .stream()
                .map(item -> new OrderCreatedEventItem(
                        item.getSkuId(),
                        item.getQuantity()
                ))
                .toList();

        OrderCreatedEvent event = new OrderCreatedEvent(
                eventId, orderId, eventItems
        );

        outboxEventRepository.save(
                OutboxEvent.createPendingEvent(
                        eventId,
                        "Order",
                        orderId,
                        "OrderCreated",
                        toJson(event)
        ));

        return new CreateOrderResult(orderId, false);
    }

    private String toJson(Object event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException ex) {
            throw new EventSerializationException("Failed to serialize outgoing order event", ex);
        }
    }
    private void validateCheckoutSnapshot(CreateOrderCommand command) {

        BigDecimal sum = BigDecimal.ZERO;

        for (var item : command.items()) {
            BigDecimal expectedItemTotalPrice =
                    item.unitPrice().multiply(BigDecimal.valueOf(item.quantity()));
            //Sanity check1 : itemTotalPrice = quantity * unitPrice
            if (expectedItemTotalPrice.compareTo(item.itemTotalPrice()) != 0) {
                throw new IllegalArgumentException("Invalid item total Price for SKU: " + item.skuId());
            }
            sum = sum.add(item.itemTotalPrice());
        }
        //sanity check2 : sum(itemTotalPrice) = totalAmount
        if (sum.compareTo(command.totalAmount()) != 0) {
            throw new IllegalArgumentException("Total amount does not match order items");
        }
    }



}
