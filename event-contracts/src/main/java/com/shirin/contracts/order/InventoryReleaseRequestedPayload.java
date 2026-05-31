package com.shirin.contracts.order;

import jakarta.validation.constraints.NotNull;
import java.util.UUID;

public record InventoryReleaseRequestedPayload(@NotNull UUID orderId) { }
