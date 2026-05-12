package com.shirin.inventory.outbox;

import lombok.AllArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@AllArgsConstructor
public class OutboxClaimService {

    private final OutboxEventRepository outboxRepository;

    @Transactional
    public List<OutboxEvent> claimOutboxEventsForPublish(int limit, String owner) {

        List<OutboxEvent> events = outboxRepository.findCandidatesForPublish(limit);

        events.forEach(event -> event.markProcessing(owner));

        return events;
    }
}
