package com.shirin.inventory.outbox;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Component
public class OutboxPublisher {

   private final OutboxClaimService claimService;
    private final OutboxStatusService statusService;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String inventoryReservedTopic;
    private final String inventoryReservationFailedTopic;
    private final int maxRetries;
    private final String owner;
    private final int claimLimit;

    public OutboxPublisher(
            OutboxClaimService claimService,
            OutboxStatusService statusService,
            KafkaTemplate<String, String> kafkaTemplate,
            @Value("${app.kafka.topics.inventory-reserved}") String inventoryReservedTopic,
            @Value("${app.kafka.topics.inventory-reservation-failed}") String inventoryReservationFailedTopic,
            @Value("${app.outbox.max-retries}") int maxRetries,
            @Value("${app.outbox.claim-limit}") int claiLimit
    ) {
        this.claimService = claimService;
        this.statusService = statusService;
        this.kafkaTemplate = kafkaTemplate;
        this.inventoryReservedTopic = inventoryReservedTopic;
        this.inventoryReservationFailedTopic = inventoryReservationFailedTopic;
        this.maxRetries = maxRetries;
        this.owner = "inventory-service-" + UUID.randomUUID();
        this.claimLimit = claiLimit;
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
        } catch (Exception ex) {

            statusService.markPublishFailed(
                    event.getId(),
                    errorMessage(ex),
                    maxRetries,
                    owner
            );
        }
    }

    private String topicFor(OutboxEvent event) {
        return
                switch (event.getEventType())
                {
                    case "InventoryReserved" -> inventoryReservedTopic;
                    case "InventoryReservationFailed" -> inventoryReservationFailedTopic;
                    default -> throw new IllegalArgumentException("Unknown event type: " + event.getEventType());
                };
    }

    private String errorMessage(Exception ex) {
        Throwable cause = ex.getCause();

        if (cause != null) {
            String message = cause.getMessage();
            return cause.getClass().getSimpleName()
                    + (message == null ? "" : ": " + message);
        }

        String message = ex.getMessage();
        return ex.getClass().getSimpleName()
                + (message == null ? "" : ": " + message);
    }

}
