package com.shirin.order.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.shirin.order.application.CreateOrderCommand;
import com.shirin.order.application.CreateOrderCommandItem;
import com.shirin.order.application.OrderApplicationService;
import jakarta.validation.Valid;
import lombok.AllArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/orders")
@AllArgsConstructor
public class OrderController {
    private final OrderApplicationService orderService;

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public CreateOrderResponse createOrder(
            @Valid @RequestBody CreateOrderRequest request) throws JsonProcessingException
    {
        CreateOrderCommand createOrderCommand = new CreateOrderCommand(
                request.items()
                        .stream()
                        .map(item -> new CreateOrderCommandItem(
                                        item.skuId(),
                                        item.quantity()))
                        .toList()
        );
        return new CreateOrderResponse(orderService.createOrder( createOrderCommand));

    }
}
