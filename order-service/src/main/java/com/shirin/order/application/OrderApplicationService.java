package com.shirin.order.application;

import com.shirin.order.domain.OrderRepository;
import lombok.AllArgsConstructor;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

@Service
@AllArgsConstructor
public class OrderApplicationService {

    private final OrderRepository orderRepository;
    private final OrderCreationTxService orderCreationTxService;
//    private final ProcessedEventRepository idempotencyRepository;

    public CreateOrderResult createOrder(CreateOrderCommand command) {
        //idempotency check
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


//    @Transactional
//    public void handleInventoryReservedEvent(InventoryReservedEvent event)
//    {
//
//        int inserted = idempotencyRepository.insertIfAbsent(event.eventId());
//        if(inserted ==0 ) return;
//
//        Order order = orderRepository.findById(event.orderId()).orElseThrow();
//        order.changeStatusToInventoryReserved();
//
//    }

//    @Transactional
//    public void handleInventoryReservationFailedEvent(InventoryReservationFailedEvent event)
//    {
//        int inserted = idempotencyRepository.insertIfAbsent(event.eventId());
//        if(inserted ==0 ) return;
//
//        Order order = orderRepository.findById(event.orderId()).orElseThrow();
//        order.changeStatusToCancelled(event.reason());
//
//
//    }




}


