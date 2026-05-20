package com.shirin.order.api;

import com.shirin.order.application.CreateOrderCommand;
import com.shirin.order.application.CreateOrderCommandItem;
import com.shirin.order.application.CreateOrderResult;
import com.shirin.order.application.OrderApplicationService;
import jakarta.validation.Valid;
import lombok.AllArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/orders")
@AllArgsConstructor
public class OrderController {
    private final OrderApplicationService orderService;

    /*
    CreateOrderRequest represents a trusted checkout snapshot produced by an upstream checkout flow.
    The project does not implement cart, catalog, pricing, or checkout services.
     */
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public CreateOrderResponse createOrder(
            @Valid @RequestBody CreateOrderRequest request)
    {
        CreateOrderCommand command = new CreateOrderCommand(
                request.requestId(),
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
        return new CreateOrderResponse(result.orderId(), result.duplicate());

    }
}
