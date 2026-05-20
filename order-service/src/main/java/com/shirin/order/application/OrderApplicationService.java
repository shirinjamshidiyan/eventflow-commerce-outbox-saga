package com.shirin.order.application;

import com.shirin.order.domain.Order;
import com.shirin.order.domain.OrderRepository;
import com.shirin.order.idempotency.ProcessedEventRepository;
import com.shirin.order.messaging.events.InventoryReleasedEvent;
import com.shirin.order.messaging.events.InventoryReservationFailedEvent;
import com.shirin.order.messaging.events.InventoryReservedEvent;
import lombok.AllArgsConstructor;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@AllArgsConstructor
public class OrderApplicationService {

    private final OrderRepository orderRepository;
    private final OrderCreationTxService orderCreationTxService;
    private final ProcessedEventRepository idempotencyRepository;

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

    @Transactional ///////////////////
    public void handleInventoryReservedEvent(InventoryReservedEvent event)
    {
        int inserted = idempotencyRepository.insertIfAbsent(event.eventId());
        if(inserted ==0 ) return;

        Order order = orderRepository.findById(event.orderId()).orElseThrow();
        order.changeStatusToInventoryReserved();


        //when payment-service is added:
        //status: INVENTORY_RESERVED --> PAYMENT_PENDING
        //event: PaymentRequested outbox event
    }

    @Transactional   //// ok
    public void handleInventoryReservationFailedEvent(InventoryReservationFailedEvent event)
    {
        int inserted = idempotencyRepository.insertIfAbsent(event.eventId());
        if(inserted ==0 ) return;

        Order order = orderRepository.findById(event.orderId()).orElseThrow();
        order.changeStatusToCancel(event.reason());

    }

    @Transactional //// ok
    public void handleInventoryReleasedEvent(InventoryReleasedEvent event)
    {
        int inserted = idempotencyRepository.insertIfAbsent(event.eventId());
        if(inserted ==0 ) return;

        Order order = orderRepository.findById(event.orderId()).orElseThrow();
        order.changeStatusToCancel("Inventory reservation released");

    }

//.............................................................

//    @Transactional
//    public void handlePaymentFailedEvent(InventoryReservedEvent event)
//    {
//        //status : CANCELLATION_PENDING
//        //event: InventoryReleased to inventory
//        int inserted = idempotencyRepository.insertIfAbsent(event.eventId());
//        if(inserted ==0 ) return;
//
//        Order order = orderRepository.findById(event.orderId()).orElseThrow();
//        order.changeStatusToInventoryReserved();
//
//    }

//    @Transactional
//    public void handlePaymentConFIRMEDEvent(InventoryReservedEvent event)
//    {
//        //status : CONFIRMED
//        //event: send to notification
//        int inserted = idempotencyRepository.insertIfAbsent(event.eventId());
//        if(inserted ==0 ) return;
//
//        Order order = orderRepository.findById(event.orderId()).orElseThrow();
//        order.changeStatusToInventoryReserved();
//
//    }


}


