package com.shirin.payment.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@Component
public class PaymentMetrics {
    private final Counter authorizedCounter;
    private final Counter failedCounter;

    public PaymentMetrics(MeterRegistry meterRegistry) {
        this.authorizedCounter = Counter.builder("payment.authorization")
                .description("Number of payment authorization attempts by result")
                .tag("result", "authorized")
                .register(meterRegistry);

        this.failedCounter = Counter.builder("payment.authorization")
                .description("Number of payment authorization attempts by result")
                .tag("result", "failed")
                .register(meterRegistry);
    }

    public void recordAuthorizedAfterCommit() {
        afterCommit( authorizedCounter::increment);
    }

    public void recordFailedAfterCommit() {
        afterCommit( failedCounter::increment);
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
