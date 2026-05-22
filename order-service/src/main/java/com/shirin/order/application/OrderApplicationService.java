package com.shirin.order.application;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.shirin.contracts.inventory.InventoryReleasedEvent;
import com.shirin.contracts.inventory.InventoryReservationFailedEvent;
import com.shirin.contracts.inventory.InventoryReservedEvent;
import com.shirin.contracts.order.InventoryReleaseRequestedEvent;
import com.shirin.contracts.order.PaymentRequestedEvent;
import com.shirin.contracts.payment.PaymentAuthorizedEvent;
import com.shirin.contracts.payment.PaymentFailedEvent;
import com.shirin.order.domain.Order;
import com.shirin.order.domain.OrderRepository;
import com.shirin.order.idempotency.ProcessedEventRepository;
import com.shirin.order.outbox.OutboxEvent;
import com.shirin.order.outbox.OutboxEventRepository;
import lombok.AllArgsConstructor;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
@AllArgsConstructor
public class OrderApplicationService {

    private final OrderRepository orderRepository;
    private final OrderCreationTxService orderCreationTxService;
    private final ProcessedEventRepository idempotencyRepository;
    private final OutboxEventRepository outboxRepository;
    private final ObjectMapper objectMapper;

    public CreateOrderResult createOrder(CreateOrderCommand command) {

        //idempotency check using command.requestId
        return orderRepository
                .findByRequestId(command.requestId())
                .map(order -> new CreateOrderResult(order.getId(), true))
                .orElseGet(() -> createOrderOrReturnDuplicate(command));
    }
    private CreateOrderResult createOrderOrReturnDuplicate(CreateOrderCommand command) {

        try {
           return orderCreationTxService.createNewOrder(command);

        }catch (DataIntegrityViolationException exception)
        {
            return orderRepository
                    .findByRequestId(command.requestId())
                    .map(order -> new CreateOrderResult(order.getId(), true) )
                    .orElseThrow(()->exception);
        }

    }

    @Transactional
    public void handleInventoryReservedEvent(InventoryReservedEvent event)
    {
        int inserted = idempotencyRepository.insertIfAbsent(event.eventId());
        if(inserted ==0 ) return;

        Order order = orderRepository.findById(event.orderId()).orElseThrow();
        boolean moved = order.moveToPaymentPendingAfterInventoryReserved();

        if(!moved) return;

        UUID eventId = UUID.randomUUID();
        PaymentRequestedEvent paymentRequestedEvent = new PaymentRequestedEvent(
                eventId,
                order.getId(),
                order.getPaymentMethodId(),
                order.getCurrency(),
                order.getTotalAmount()
        );

        outboxRepository.save(
                OutboxEvent.createPendingEvent(
                        eventId,
                        "Order",
                        order.getId(),
                        "PaymentRequested",
                        toJson(paymentRequestedEvent)
        ));

    }

    @Transactional
    public void handleInventoryReservationFailedEvent(InventoryReservationFailedEvent event)
    {
        int inserted = idempotencyRepository.insertIfAbsent(event.eventId());
        if(inserted ==0 ) return;

        Order order = orderRepository.findById(event.orderId()).orElseThrow();
        order.cancelDirectly(event.reason());

    }

    @Transactional
    public void handlePaymentAuthorizedEvent(PaymentAuthorizedEvent event)
    {
        int inserted = idempotencyRepository.insertIfAbsent(event.eventId());
        if(inserted ==0 ) return;

        Order order = orderRepository.findById(event.orderId()).orElseThrow();
        order.confirmPayment(event.paymentId());
        //event: send to notification

    }

    @Transactional
    public void handleInventoryReleasedEvent(InventoryReleasedEvent event)
    {
        int inserted = idempotencyRepository.insertIfAbsent(event.eventId());
        if(inserted ==0 ) return;

        Order order = orderRepository.findById(event.orderId()).orElseThrow();
        order.completeCancellation("Inventory reservation released");

    }

    @Transactional
    public void handlePaymentFailedEvent(PaymentFailedEvent event)
    {
        int inserted = idempotencyRepository.insertIfAbsent(event.eventId());
        if(inserted ==0 ) return;

        Order order = orderRepository.findById(event.orderId()).orElseThrow();
        boolean cancellationStarted = order.startCancellation(event.reason());

        if (!cancellationStarted) {
            return;
        }

        UUID eventId = UUID.randomUUID();
        InventoryReleaseRequestedEvent releaseRequestedEvent  = new InventoryReleaseRequestedEvent(
                eventId, order.getId()
        );
        outboxRepository.save(
                OutboxEvent.createPendingEvent(
                        eventId,
                        "Order",
                        order.getId(),
                        "InventoryReleaseRequested",
                        toJson(releaseRequestedEvent)

                )
        );

    }

    private String toJson(Object event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException ex) {
            throw new EventSerializationException("Failed to serialize outgoing order event", ex);
        }
    }

}


