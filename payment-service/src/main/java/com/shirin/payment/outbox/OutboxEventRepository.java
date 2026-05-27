package com.shirin.payment.outbox;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {

    // status = 'FAILED' AND next_retry_at IS NULL : defensive query
    @Query(value = """
        SELECT *
        FROM outbox_events
        WHERE (
            status = 'PENDING'
            OR
            (
                status = 'FAILED'
                AND (next_retry_at IS NULL OR next_retry_at <= NOW())
            )
        )
        ORDER BY created_at ASC
        LIMIT :limit
        FOR UPDATE SKIP LOCKED
        """,
            nativeQuery = true)
    List<OutboxEvent> findCandidatesForPublish(@Param("limit") int limit);

    @Modifying
    @Query(value = """
        UPDATE outbox_events
        SET status = 'FAILED',
            last_error = :error,
            next_retry_at = NOW(),
            processing_started_at = NULL,
            processing_by = NULL
        WHERE status = 'PROCESSING'
          AND processing_started_at < :threshold
        """, nativeQuery = true)
    int resetStuckProcessingEvents(
            @Param("threshold") Instant threshold,
            @Param("error") String error
    );

    long countByStatus(OutboxStatus status);


}

