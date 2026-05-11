package com.shirin.inventory.outbox;

import lombok.AllArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
@AllArgsConstructor
public class OutboxStatusService {

    private final OutboxEventRepository outboxRepository;

    @Transactional
    public void markPublished(UUID eventId, String owner) {
        OutboxEvent event = outboxRepository.findById(eventId).orElseThrow();
        if (!event.isProcessingBy(owner)) {
            return;
        }
        event.markPublished();
    }

    @Transactional
    public void markPublishFailed(UUID eventId, String error, int maxRetries, String owner) {
        OutboxEvent event = outboxRepository.findById(eventId).orElseThrow();
        if (!event.isProcessingBy(owner)) {
            return;
        }
        event.markFailedOrDead(error, maxRetries);
    }

}
