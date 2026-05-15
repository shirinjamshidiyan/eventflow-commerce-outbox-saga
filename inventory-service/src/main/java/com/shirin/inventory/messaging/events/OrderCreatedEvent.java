package com.shirin.inventory.messaging.events;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.util.List;
import java.util.UUID;


public record OrderCreatedEvent(
       @NotNull UUID eventId,
       @NotNull UUID orderId,
        @NotEmpty List<@Valid OrderCreatedEventItem> items) { }
