package com.shirin.order.outbox;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {

    // Publisher must read events that: status = PENDING or (status = FAILED and next_retry_at <= now)
    @Query("""
            select e from OutboxEvent e
            where e.status in :statuses
            and (e.nextRetryAt is null or e.nextRetryAt <= :now)
            order by e.createdAt asc
            """)
    List<OutboxEvent> findPublishableEvents(
            @Param("statuses") Collection<OutboxStatus> statuses,
            @Param("now") Instant now,
            Pageable pageable
    );

}

