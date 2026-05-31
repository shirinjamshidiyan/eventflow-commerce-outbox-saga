package com.shirin.contracts.inventory;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.util.UUID;

public record InventoryReservationFailedPayload( @NotNull UUID orderId, @NotBlank String reason) { }
