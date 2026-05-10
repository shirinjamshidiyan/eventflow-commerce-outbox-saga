package com.shirin.order.outbox;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class OutboxPublisher {

   private final OutboxClaimService claimService;
    private final OutboxStatusService statusService;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String orderCreatedTopic;
    private final int maxRetries;
    private final String owner;
    private final int claiLimit;

    public OutboxPublisher(
            OutboxClaimService claimService,
            OutboxStatusService statusService,
            KafkaTemplate<String, String> kafkaTemplate,
            @Value("${app.kafka.topics.order-created}") String orderCreatedTopic,
            @Value("${app.outbox.max-retries}") int maxRetries,
            @Value("${app.outbox.claim-limit}") int claiLimit
    ) {
        this.claimService = claimService;
        this.statusService = statusService;
        this.kafkaTemplate = kafkaTemplate;
        this.orderCreatedTopic = orderCreatedTopic;
        this.maxRetries = maxRetries;
        this.owner = "order-service-" + UUID.randomUUID();
        this.claiLimit = claiLimit;
    }


    @Scheduled(fixedDelayString = "${app.outbox.fixed-delay-ms}")
    public void publishCandidateOutboxEvents() {
        System.out.println("------------------" + Instant.now());
        List<OutboxEvent> events = claimService.claimOutboxEventsForPublish(claiLimit, owner);

        for (OutboxEvent event : events) {
            publish(event);
        }
    }


    private void publish(OutboxEvent event) {
        try {
            log.info("Publishing outbox event {} to Kafka", event.getId());
            kafkaTemplate
                    .send(topicFor(event), event.getAggregateId().toString() , event.getPayload())
                    .get(5, TimeUnit.SECONDS);

            statusService.markPublished(event.getId(), owner);
            log.info("Published outbox event {}", event.getId());
        } catch (Exception ex) {

            statusService.markPublishFailed(
                    event.getId(),
                    errorMessage(ex),
                    maxRetries,
                    owner
            );
            log.warn("Failed to publish outbox event {}", event.getId(), ex);
        }
    }

    private String topicFor(OutboxEvent event) {
        return
                switch (event.getEventType())
                {
                    case "OrderCreated" -> orderCreatedTopic;
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
