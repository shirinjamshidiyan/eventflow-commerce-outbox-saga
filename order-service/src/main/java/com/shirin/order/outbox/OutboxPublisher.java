package com.shirin.order.outbox;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Component
public class OutboxPublisher {

    private final OutboxEventRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String OrderCreatedTopic;
    private final int maxRetries;

    public OutboxPublisher(
            OutboxEventRepository outboxRepository,
            KafkaTemplate<String, String> kafkaTemplate,
            @Value("${app.kafka.topics.order-created}") String orderCreatedTopic,
            @Value("${app.outbox.max-retries}") int maxRetries
    ) {
        this.outboxRepository = outboxRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.OrderCreatedTopic = orderCreatedTopic;
        this.maxRetries = maxRetries;
    }

    @Scheduled(fixedDelayString = "${app.outbox.fixed-delay-ms}")
    public void publishPendingEvents() {
        List<OutboxEvent> events = outboxRepository.findPublishableEvents(
                List.of(OutboxStatus.PENDING, OutboxStatus.FAILED),
                Instant.now(),
                PageRequest.of(0, 20)
        );

        for (OutboxEvent event : events) {
            publish(event);
        }
    }

    private void publish(OutboxEvent event) {
        try {
            kafkaTemplate
                    .send(OrderCreatedTopic, event.getAggregateId().toString() , event.getPayload())
                    .get(5, TimeUnit.SECONDS);

            event.markPublished();
            outboxRepository.save(event);
        } catch (Exception ex) {
            event.markPublishFailed(errorMessage(ex), maxRetries);
            outboxRepository.save(event);
        }
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
