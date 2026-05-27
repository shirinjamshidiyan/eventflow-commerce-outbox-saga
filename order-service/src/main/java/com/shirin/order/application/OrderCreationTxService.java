package com.shirin.order.application;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.shirin.contracts.common.EventEnvelope;
import com.shirin.contracts.common.EventSources;
import com.shirin.contracts.common.EventTypes;
import com.shirin.contracts.order.OrderCreatedItemsPayload;
import com.shirin.contracts.order.OrderCreatedPayload;
import com.shirin.order.domain.Order;
import com.shirin.order.domain.OrderRepository;
import com.shirin.order.observability.OrderMetrics;
import com.shirin.order.outbox.OutboxEvent;
import com.shirin.order.outbox.OutboxEventRepository;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

@Service
@AllArgsConstructor
@Slf4j
public class OrderCreationTxService  {

    private final OrderRepository orderRepository;
    private final OrderNumberGenerator orderNumberGenerator;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;
    private final OrderMetrics orderMetrics;

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

        List<OrderCreatedItemsPayload> payloadItems = order
                .getItems()
                .stream()
                .map(item -> new OrderCreatedItemsPayload(
                        item.getSkuId(),
                        item.getQuantity()
                )).toList();

        OrderCreatedPayload payload= new OrderCreatedPayload(orderId, payloadItems);

        EventEnvelope<OrderCreatedPayload> newEnvelope =
                EventEnvelope.create(
                        eventId,
                        EventTypes.ORDER_CREATED,
                        1,
                        command.correlationId(),
                        null,
                        EventSources.ORDER_SERVICE,
                        payload
        );

        outboxEventRepository.save(
                OutboxEvent.createPendingEvent(
                        eventId,
                        "Order",
                        orderId,
                        EventTypes.ORDER_CREATED,
                        toJson(newEnvelope)
        ));

        orderMetrics.recordCreatedAfterCommit();

        log.info("Order created event stored in outbox");

        return new CreateOrderResult(orderId, false);
    }

    private String toJson(Object envelope) {
        try {
            return objectMapper.writeValueAsString(envelope);
        } catch (JsonProcessingException ex) {
            throw new EventSerializationException("Failed to serialize outgoing order envelope", ex);
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
