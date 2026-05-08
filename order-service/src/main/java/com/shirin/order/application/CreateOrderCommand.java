package com.shirin.order.application;

import java.util.List;

public record CreateOrderCommand(List<CreateOrderCommandItem> items) {
}
