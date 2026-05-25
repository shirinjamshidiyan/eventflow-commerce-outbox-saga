package com.shirin.order.api;

import com.shirin.order.application.CreateOrderCommand;
import com.shirin.order.application.CreateOrderCommandItem;
import com.shirin.order.application.CreateOrderResult;
import com.shirin.order.application.OrderApplicationService;
import com.shirin.order.observability.LoggingContext;
import jakarta.validation.Valid;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/orders")
@AllArgsConstructor
@Slf4j
public class OrderController {
    private final OrderApplicationService orderService;

    /*
    CreateOrderRequest represents a trusted checkout snapshot produced by an upstream checkout flow.
    The project does not implement cart, catalog, pricing, or checkout services.
     */
    @PostMapping
    public ResponseEntity<CreateOrderResponse> createOrder(
           @Valid @RequestBody CreateOrderRequest request)
    {

        LoggingContext.putRequestId(request.requestId());

        log.info("Received create order request");
        CreateOrderCommand command = new CreateOrderCommand(
                request.requestId(),
                LoggingContext.currentCorrelationId(),
                request.checkoutId(),
                request.customerId(),
                request.paymentMethodId(),
                request.currency(),
                request.totalAmount(),
                request.items()
                        .stream()
                        .map(item -> new CreateOrderCommandItem(
                                item.skuId(),
                                item.productName(),
                                item.quantity(),
                                item.unitPrice(),
                                item.itemTotalPrice()
                        ))
                        .toList()
        );
        CreateOrderResult result = orderService.createOrder(command);

        LoggingContext.putOrderId(result.orderId());

        log.info("Create order request handled");

        return ResponseEntity
                .status(HttpStatus.CREATED)
                .body(new CreateOrderResponse(
                        result.orderId(),
                        result.duplicate()
                ));

    }
}
