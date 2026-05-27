package com.shirin.inventory.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@Component
public class InventoryMetrics {

    private final Counter reservationReservedCounter;
    private final Counter reservationFailedCounter;
    private final Counter releaseCompletedCounter;


    public InventoryMetrics(MeterRegistry meterRegistry)
    {
        this.reservationReservedCounter = Counter.builder("inventory.reservation")
                .description("Number of inventory reservation attempts by result")
                .tag("result", "reserved")
                .register(meterRegistry);

        this.reservationFailedCounter = Counter.builder("inventory.reservation")
                .description("Number of inventory reservation attempts by result")
                .tag("result", "failed")
                .register(meterRegistry);

        this.releaseCompletedCounter = Counter.builder("inventory.release")
                .description("Number of completed inventory release requests")
                .tag("result", "completed")
                .register(meterRegistry);

    }


    public void recordReservationReservedAfterCommit() {
        afterCommit(reservationReservedCounter::increment);
    }

    public void recordReservationFailedAfterCommit() {
        afterCommit(reservationFailedCounter::increment);
    }

    public void recordReleaseCompletedAfterCommit() {
        afterCommit(releaseCompletedCounter::increment);
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
