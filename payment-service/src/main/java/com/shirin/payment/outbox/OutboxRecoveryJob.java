package com.shirin.payment.outbox;

import lombok.AllArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;

@Component
@AllArgsConstructor
public class OutboxRecoveryJob {
    private final OutboxEventRepository outboxRepository;
    @Transactional
    @Scheduled(fixedDelayString = "${app.outbox.recovery-delay-ms}")
    public void recoverStuckProcessingEvents() {
        Instant threshold = Instant.now().minus(Duration.ofMinutes(2));

        outboxRepository.resetStuckProcessingEvents(
                threshold,
                "Event was stuck in PROCESSING and was reset for retry"
        );
    }
}
