package com.shirin.inventory.observability;

import com.shirin.inventory.outbox.OutboxEventRepository;
import com.shirin.inventory.outbox.OutboxStatus;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.Locale;

@Component
public class OutboxMetrics {

    private final Counter successPublishCounter;
    private final Counter failedPublishCounter;

    public OutboxMetrics(MeterRegistry meterRegistry, OutboxEventRepository outboxRepository)
    {

        this.successPublishCounter = Counter.builder("outbox.publish")
                .description("Number of outbox publish attempts by result")
                .tag("result", "success")
                .register(meterRegistry);

        this.failedPublishCounter = Counter.builder("outbox.publish")
                .description("Number of outbox publish attempts by result")
                .tag("result", "failed")
                .register(meterRegistry);

        for (OutboxStatus status : OutboxStatus.values()) {

            Gauge.builder("outbox.events", () -> outboxRepository.countByStatus(status))
                    .description("Current number of outbox events by status")
                    .tag("status", status.name().toLowerCase(Locale.ROOT))
                    .register(meterRegistry);
        }
    }

    public void recordPublished() {
        successPublishCounter.increment();
    }

    public void recordPublishFailed() {
        failedPublishCounter.increment();
    }


}
