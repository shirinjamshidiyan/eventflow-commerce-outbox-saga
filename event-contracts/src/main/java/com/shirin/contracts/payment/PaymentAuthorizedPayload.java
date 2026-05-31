package com.shirin.contracts.payment;

import jakarta.validation.constraints.NotNull;
import java.util.UUID;

public record PaymentAuthorizedPayload(@NotNull UUID orderId, @NotNull UUID paymentId) { }
