package com.shirin.order.outbox;

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
    private final String orderCreatedTopic;
    private final int maxRetries;
    private final String owner;
    private final int claimLimit;

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

        } catch (InterruptedException ex) {
            try {
                statusService.markPublishFailed(
                        event.getId(),
                        errorMessage(ex),
                        maxRetries,
                        owner
                );
            }
            finally {
                Thread.currentThread().interrupt(); //restore interrupt flag
            }
        } catch (TimeoutException | ExecutionException ex) {
            statusService.markPublishFailed(
                    event.getId(),
                    errorMessage(ex),
                    maxRetries,
                    owner
            );
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
                    case "OrderCreated" -> orderCreatedTopic;
                    default -> throw new IllegalArgumentException("Unknown event type: " + event.getEventType());
                };
    }


}
