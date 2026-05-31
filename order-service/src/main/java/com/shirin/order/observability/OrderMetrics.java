package com.shirin.order.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionSynchronization;
import java.time.Duration;

@Component
public class OrderMetrics {

    private final Counter createdCounter;
    private final Counter confirmedCounter;
    private final Counter cancelledCounter;
    private final Counter cancellationStartedCounter;
    private final Timer confirmedResolutionTimer;
    private final Timer cancelledResolutionTimer;

    public OrderMetrics(MeterRegistry meterRegistry)
    {
        createdCounter = Counter.builder("order.saga.created")
                .description("Number of newly created orders")
                .register(meterRegistry);

        this.confirmedCounter = Counter.builder("order.saga.confirmed")
                .description("Number of orders confirmed by the saga")
                .register(meterRegistry);

        this.cancelledCounter = Counter.builder("order.saga.cancelled")
                .description("Number of orders cancelled by the saga")
                .register(meterRegistry);

        this.cancellationStartedCounter = Counter.builder("order.saga.cancellation.started")
                .description("Number of orders where cancellation flow started")
                .register(meterRegistry);

        this.confirmedResolutionTimer = Timer.builder("order.resolution.duration")
                .description("Time from order creation until terminal order outcome")
                .tag("outcome", "confirmed")
                .register(meterRegistry);

        this.cancelledResolutionTimer = Timer.builder("order.resolution.duration")
                .description("Time from order creation until terminal order outcome")
                .tag("outcome", "cancelled")
                .register(meterRegistry);

    }

    public void recordCreatedAfterCommit() {
        afterCommit(createdCounter::increment);
    }

    public void recordConfirmedAfterCommit(Duration resolutionDuration) {
        afterCommit(() -> {
            confirmedCounter.increment();
            confirmedResolutionTimer.record(resolutionDuration);
        });
    }

    public void recordCancelledAfterCommit(Duration resolutionDuration) {
        afterCommit(() -> {
            cancelledCounter.increment();
            cancelledResolutionTimer.record(resolutionDuration);
        });
    }

    public void recordCancellationStartedAfterCommit() {
        afterCommit(cancellationStartedCounter::increment);
    }

    private void afterCommit(Runnable action) {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(
                    new TransactionSynchronization() {
                        @Override
                        public void afterCommit() {
                            action.run();
                        }
                    }
            );
        } else {
            action.run();
        }
    }
}
