package com.shirin.order.application;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.shirin.contracts.common.EventEnvelope;
import com.shirin.contracts.common.EventSources;
import com.shirin.contracts.common.EventTypes;
import com.shirin.contracts.inventory.InventoryReleasedPayload;
import com.shirin.contracts.inventory.InventoryReservationFailedPayload;
import com.shirin.contracts.inventory.InventoryReservedPayload;
import com.shirin.contracts.order.InventoryReleaseRequestedPayload;
import com.shirin.contracts.order.PaymentRequestedPayload;
import com.shirin.contracts.payment.PaymentAuthorizedPayload;
import com.shirin.contracts.payment.PaymentFailedPayload;
import com.shirin.order.domain.Order;
import com.shirin.order.domain.OrderRepository;
import com.shirin.order.idempotency.ProcessedEventRepository;
import com.shirin.order.outbox.OutboxEvent;
import com.shirin.order.outbox.OutboxEventRepository;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
@AllArgsConstructor
@Slf4j
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
    public void handleInventoryReservedEvent(EventEnvelope<InventoryReservedPayload> envelope)
    {
        int inserted = idempotencyRepository.insertIfAbsent(envelope.eventId());
        if (inserted == 0) {
            log.info("Duplicate inventory reserved event ignored");
            return;
        }

        Order order = orderRepository.findById(envelope.payload().orderId()).orElseThrow();
        boolean moved = order.moveToPaymentPendingAfterInventoryReserved();

        if (!moved) {
            log.info("move To payment pending after InventoryReserved was not allowed");
            return;
        }

        log.info("Order moved to payment pending");

        UUID eventId = UUID.randomUUID();

        PaymentRequestedPayload payload = new PaymentRequestedPayload(
                order.getId(),
                order.getPaymentMethodId(),
                order.getCurrency(),
                order.getTotalAmount()
        );
        EventEnvelope<PaymentRequestedPayload> newEnvelope = EventEnvelope.create(
                eventId,
                EventTypes.PAYMENT_REQUESTED,
                1,
                envelope.correlationId(),
                envelope.eventId(),
                EventSources.ORDER_SERVICE,
                payload);


        outboxRepository.save(
                OutboxEvent.createPendingEvent(
                        eventId,
                        "Order",
                        order.getId(),
                        EventTypes.PAYMENT_REQUESTED,
                        toJson(newEnvelope)
        ));
        log.info("Payment requested event stored in outbox");

    }

    @Transactional
    public void handleInventoryReservationFailedEvent(EventEnvelope<InventoryReservationFailedPayload> envelope)
    {
        int inserted = idempotencyRepository.insertIfAbsent(envelope.eventId());
        if (inserted == 0) {
            log.info("Duplicate Inventory reservation failed event ignored");
            return;
        }

        Order order = orderRepository.findById(envelope.payload().orderId()).orElseThrow();
        order.cancelDirectly(envelope.payload().reason());
        log.info("Order cancelled because inventory reservation failed");

    }

    @Transactional
    public void handlePaymentAuthorizedEvent(EventEnvelope<PaymentAuthorizedPayload> envelope)
    {
        int inserted = idempotencyRepository.insertIfAbsent(envelope.eventId());
        if (inserted == 0) {
            log.info("Duplicate payment authorized event ignored");
            return;
        }

        Order order = orderRepository.findById(envelope.payload().orderId()).orElseThrow();
        order.confirmPayment(envelope.payload().paymentId());
        log.info("Order confirmed after payment authorization");
        //event: send to notification

    }

    @Transactional
    public void handleInventoryReleasedEvent(EventEnvelope<InventoryReleasedPayload> envelope)
    {
        int inserted = idempotencyRepository.insertIfAbsent(envelope.eventId());
        if (inserted == 0) {
            log.info("Duplicate inventory released event ignored");
            return;
        }

        Order order = orderRepository.findById(envelope.payload().orderId()).orElseThrow();
        order.completeCancellation("Inventory reservation released");
        log.info("Order cancelled after inventory release");

    }

    @Transactional
    public void handlePaymentFailedEvent(EventEnvelope<PaymentFailedPayload> envelope)
    {
        int inserted = idempotencyRepository.insertIfAbsent(envelope.eventId());
        if (inserted == 0) {
            log.info("Duplicate payment failed event ignored");
            return;
        }

        Order order = orderRepository.findById(envelope.payload().orderId()).orElseThrow();
        boolean cancellationStarted = order.startCancellation(envelope.payload().reason());

        if (!cancellationStarted) {
            log.info("Payment failed event ignored because order state does not allow cancellation");
            return;
        }
        log.info("Order moved to cancellation pending after payment failure");

        UUID eventId = UUID.randomUUID();
        InventoryReleaseRequestedPayload payload  = new InventoryReleaseRequestedPayload(order.getId());

        EventEnvelope<InventoryReleaseRequestedPayload> newEnvelope = EventEnvelope.create(
                eventId,
                EventTypes.INVENTORY_RELEASE_REQUESTED,
                1,
                envelope.correlationId(),
                envelope.eventId(),
                EventSources.ORDER_SERVICE,
                payload);

        outboxRepository.save(
                OutboxEvent.createPendingEvent(
                        eventId,
                        "Order",
                        order.getId(),
                        EventTypes.INVENTORY_RELEASE_REQUESTED,
                        toJson(newEnvelope))
        );

        log.info("Inventory release requested event stored in outbox");
    }

    private String toJson(Object envelope) {
        try {
            return objectMapper.writeValueAsString(envelope);
        } catch (JsonProcessingException ex) {
            throw new EventSerializationException("Failed to serialize outgoing order envelope", ex);
        }
    }

}


