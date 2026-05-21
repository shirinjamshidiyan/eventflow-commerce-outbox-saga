package com.shirin.payment.outbox;

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
    private final String paymentAuthorizedTopic;
    private final String paymentFailedTopic;
    private final int maxRetries;
    private final String owner;
    private final int claimLimit;

    public OutboxPublisher(
            OutboxClaimService claimService,
            OutboxStatusService statusService,
            KafkaTemplate<String, String> kafkaTemplate,
            @Value("${app.kafka.topics.payment-authorized}") String paymentAuthorizedTopic,
            @Value("${app.kafka.topics.payment-failed}") String paymentFailedTopic,
            @Value("${app.outbox.max-retries}") int maxRetries,
            @Value("${app.outbox.claim-limit}") int claimLimit
    ) {
        this.claimService = claimService;
        this.statusService = statusService;
        this.kafkaTemplate = kafkaTemplate;
        this.paymentAuthorizedTopic = paymentAuthorizedTopic;
        this.paymentFailedTopic = paymentFailedTopic;
        this.maxRetries = maxRetries;
        this.owner = "payment-service-" + UUID.randomUUID();
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
                Thread.currentThread().interrupt(); //restore interrupt flag
            }
        } catch (TimeoutException | ExecutionException | RuntimeException ex) {
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
                    case "PaymentAuthorized" -> paymentAuthorizedTopic;
                    case "PaymentFailed" -> paymentFailedTopic;
                    default -> throw new IllegalArgumentException("Unknown event type: " + event.getEventType());
                };
    }


}
