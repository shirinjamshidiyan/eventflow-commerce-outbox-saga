package com.shirin.inventory.outbox;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
public class OutboxPublisher {

   private final OutboxClaimService claimService;
    private final OutboxStatusService statusService;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String inventoryReservedTopic;
    private final String inventoryReservationFailedTopic;
    private final String inventoryReleasedTopic;
    private final int maxRetries;
    private final String owner;
    private final int claimLimit;

    public OutboxPublisher(
            OutboxClaimService claimService,
            OutboxStatusService statusService,
            KafkaTemplate<String, String> kafkaTemplate,
            @Value("${app.kafka.topics.inventory-reserved}") String inventoryReservedTopic,
            @Value("${app.kafka.topics.inventory-reservation-failed}") String inventoryReservationFailedTopic,
            @Value("${app.kafka.topics.inventory-released}") String inventoryReleasedTopic,
            @Value("${app.outbox.max-retries}") int maxRetries,
            @Value("${app.outbox.claim-limit}") int claimLimit
    ) {
        this.claimService = claimService;
        this.statusService = statusService;
        this.kafkaTemplate = kafkaTemplate;
        this.inventoryReservedTopic = inventoryReservedTopic;
        this.inventoryReservationFailedTopic = inventoryReservationFailedTopic;
        this.inventoryReleasedTopic = inventoryReleasedTopic;
        this.maxRetries = maxRetries;
        this.owner = "inventory-service-" + UUID.randomUUID();
        this.claimLimit = claimLimit;
    }


    @Scheduled(fixedDelayString = "${app.outbox.fixed-delay-ms}")
    public void publishCandidateOutboxEvents() {

        List<OutboxEvent> events = claimService.claimOutboxEventsForPublish(claimLimit, owner);

        for (OutboxEvent event : events) {
            publish(event);
        }
    }


    private void publish(OutboxEvent event) {
        try {
            kafkaTemplate
                    .send(topicFor(event), event.getAggregateId().toString() , event.getPayload())
                    .get(5, TimeUnit.SECONDS);

            statusService.markPublished(event.getId(), owner);

        } catch (InterruptedException ex) {
            try {
                statusService.markPublishFailed(
                        event.getId(),
                        errorMessage(ex),
                        maxRetries,
                        owner
                );
            } finally {
                // Restore the interrupt status because catching InterruptedException clears it.
                Thread.currentThread().interrupt();
            }

        } catch (ExecutionException | TimeoutException | RuntimeException ex) {
            statusService.markPublishFailed(
                    event.getId(),
                    errorMessage(ex),
                    maxRetries,
                    owner
            );
            //broker unavailable -> ExecutionException, cause is Kafka exception
            //serialization failed -> ExecutionException, cause is serialization related exception
            //invalid/missing topic -> ExecutionException, cause is Kafka exception
            //send did not complete within 5 seconds -> TimeoutException
        }
    }
    private String errorMessage(Exception ex) {

        Throwable target = ex.getCause() != null ? ex.getCause() : ex;
        String message = target.getMessage();

        return target.getClass().getSimpleName()
                + (message == null ? "" : ": " + message);
    }

    private String topicFor(OutboxEvent event) {
        return
                switch (event.getEventType())
                {
                    case "InventoryReserved" -> inventoryReservedTopic;
                    case "InventoryReservationFailed" -> inventoryReservationFailedTopic;
                    case "InventoryReleased" -> inventoryReleasedTopic;
                    default -> throw new IllegalArgumentException("Unknown event type: " + event.getEventType());
                };
    }

}
