package com.shirin.inventory.application;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.shirin.inventory.domain.InventoryItem;
import com.shirin.inventory.domain.InventoryItemRepository;
import com.shirin.inventory.idempotency.ProcessedEventRepository;
import com.shirin.inventory.messaging.events.InventoryReservationFailedEvent;
import com.shirin.inventory.messaging.events.InventoryReservedEvent;
import com.shirin.inventory.messaging.events.OrderCreatedEvent;
import com.shirin.inventory.messaging.events.OrderCreatedEventItem;
import com.shirin.inventory.outbox.OutboxEvent;
import com.shirin.inventory.outbox.OutboxEventRepository;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

@Service
@AllArgsConstructor
public class InventoryApplicationService {

    private final ProcessedEventRepository idempotencyRepository;
    private final InventoryItemRepository inventoryRepository;
    private final OutboxEventRepository outboxRepository;
    private final ObjectMapper objectMapper;

    @Transactional
   public void processOrderCreatedEvent(OrderCreatedEvent event)  {

        // processed_events insert
        int inserted = idempotencyRepository.insertIfAbsent(event.eventId());
        if(inserted==0)
            return;

        //for reducing the possibility of deadlock between multiple orders
        // Arrange SKUs in order, then process and lock sorted items
        List<OrderCreatedEventItem> sortedItems = event.items()
                .stream()
                .sorted(Comparator.comparing(item -> item.skuId().toString()))
                .toList();

        List<InventoryItem> lockedItems = new ArrayList<>();
        boolean reserved = true;
        String failureReason = null;

        // inventory row lock
        for (var item: sortedItems)
        {
          InventoryItem inventoryItem = inventoryRepository
                  .findBySkuIdAndLock(item.skuId())
                  .orElse(null);

          if(inventoryItem == null)
          {
              reserved = false;
              failureReason = "SKU not found: " + item.skuId();
              break;
          }

          if (!inventoryItem.hasEnoughQuantity(item.quantity())) {
              reserved = false;
              failureReason = "Not enough stock for SKU: " + item.skuId();
              break;
          }
            lockedItems.add(inventoryItem);
       }
      if(reserved)
      {
          // stock decrease
          for (int i = 0; i < event.items().size(); i++) {
              lockedItems.get(i).decrease(event.items().get(i).quantity());
          }

        InventoryReservedEvent successEvent = new InventoryReservedEvent(
                UUID.randomUUID(),
                event.orderId()
        );
        //  result outbox insert
        outboxRepository.save(OutboxEvent.createPendingEvent(
                successEvent.eventId(),
                "Inventory",
                event.orderId(),
                "InventoryReserved",
                toJson(successEvent)
         ));
      } else {
        InventoryReservationFailedEvent failureEvent  = new InventoryReservationFailedEvent(
                UUID.randomUUID(), event.orderId(), failureReason);

          //  result outbox insert
        outboxRepository.save(OutboxEvent.createPendingEvent(
                failureEvent.eventId(),
                "Inventory",
                event.orderId(),
                "InventoryReservationFailed",
                toJson(failureEvent)
        ));
    }

    }

    //change Checked Exception(JsonProcessingException) to Unchecked (IllegalStateException),
    // so that @Transaction and rollback will work on it
    private String toJson(Object event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException ex) {
            throw new EventSerializationException("Failed to serialize outgoing inventory event", ex);
        }
    }
}
