package com.shirin.inventory.application;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.shirin.inventory.domain.*;
import com.shirin.inventory.idempotency.ProcessedEventRepository;
import com.shirin.inventory.messaging.events.*;
import com.shirin.inventory.outbox.OutboxEvent;
import com.shirin.inventory.outbox.OutboxEventRepository;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.util.*;
import java.util.stream.Collectors;

@Service
@AllArgsConstructor
public class InventoryApplicationService {

    private final ProcessedEventRepository idempotencyRepository;
    private final InventoryItemRepository inventoryRepository;
    private final InventoryReservationRepository reservationRepository;
    private final OutboxEventRepository outboxRepository;
    private final ObjectMapper objectMapper;

    @Transactional
   public void processOrderCreatedEvent(OrderCreatedEvent event)  {

        // idempotency check : processed_events insert
        int inserted = idempotencyRepository.insertIfAbsent(event.eventId());
        if(inserted==0)
            return;

        // Group requested quantities by SKU and sum duplicate SKU entries
        Map<UUID, Integer> requestedBySku = event.items().stream()
                .collect(Collectors.toMap(
                        OrderCreatedEventItem::skuId,
                        OrderCreatedEventItem::quantity,
                        Integer::sum
                ));

        //Sort SKUs to reduce deadlock risk during inventory locking.
        List<Map.Entry<UUID, Integer>>  sortedRequests = requestedBySku
                .entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey())
                .toList();


        // Keep locked inventory items in the same order as the sorted SKU requests (using LinkedHashMap)
        Map<UUID, InventoryItem> lockedInventoryItems = new LinkedHashMap<>();
        boolean reserved = true;
        String failureReason = null;

        // inventory row lock
        for (var item: sortedRequests)
        {
           UUID itemSkuId = item.getKey();
           Integer itemQuantity = item.getValue();

           InventoryItem inventoryItem = inventoryRepository
                  .findBySkuIdAndLock(itemSkuId)
                  .orElse(null);

          if(inventoryItem == null)
          {
              reserved = false;
              failureReason = "SKU not found: " + itemSkuId;
              break;
          }

          if (!inventoryItem.hasEnoughQuantity(itemQuantity)) {
              reserved = false;
              failureReason = "Not enough stock for SKU: " + itemSkuId;
              break;
          }
            lockedInventoryItems.put(itemSkuId,inventoryItem );
       }

      if(reserved)
      {
          for(var requestItem : sortedRequests)
          {
              UUID desiredSkuId = requestItem.getKey();
              Integer desiredQuantity = requestItem.getValue();
              // stock decrease
              lockedInventoryItems.get(desiredSkuId).decrease(desiredQuantity);

              // make a reservation
              reservationRepository.save(
                      new InventoryReservation(
                              UUID.randomUUID(),
                              event.orderId(),
                              desiredSkuId,
                              desiredQuantity
                      )
              );

          }

        InventoryReservedEvent successEvent = new InventoryReservedEvent(
                UUID.randomUUID(),
                event.orderId()
        );
        //  result outbox insert
        outboxRepository.save(
                OutboxEvent.createPendingEvent(
                        successEvent.eventId(),
                        "Inventory",
                        event.orderId(),
                        "InventoryReserved",
                        toJson(successEvent)
         ));
      } else
      {
         InventoryReservationFailedEvent failureEvent  = new InventoryReservationFailedEvent(
                UUID.randomUUID(), event.orderId(), failureReason);

          //  result outbox insert
         outboxRepository.save(
                OutboxEvent.createPendingEvent(
                        failureEvent.eventId(),
                        "Inventory",
                        event.orderId(),
                        "InventoryReservationFailed",
                        toJson(failureEvent)
        ));
    }
   }

    @Transactional
    public void processInventoryReleaseRequestedEvent(InventoryReleaseRequestedEvent event) {

        // idempotency check : processed_events insert
        int inserted = idempotencyRepository.insertIfAbsent(event.eventId());
        if(inserted==0)
            return;

        List<InventoryReservation> sortedList = reservationRepository
                .findAllByOrderIdAndStatusForUpdate(
                        event.orderId(),
                        InventoryReservationStatus.RESERVED
                );

        for(InventoryReservation reservation : sortedList)
        {
            InventoryItem lockedInventoryItem = inventoryRepository
                    .findBySkuIdAndLock(reservation.getSkuId())
                    .orElseThrow();

            lockedInventoryItem.increase(reservation.getQuantity());
            reservation.release();

        }

        // Release is treated as idempotent. If no reserved rows exist (= sortedList.size=0)
        // publishing InventoryReleased allows the saga to continue.
        UUID eventId = UUID.randomUUID();

        InventoryReleasedEvent successEvent = new InventoryReleasedEvent(
               eventId , event.orderId()
        );
        outboxRepository.save(
                OutboxEvent.createPendingEvent(
                        eventId,
                        "Inventory",
                        event.orderId(),
                        "InventoryReleased",
                        toJson(successEvent)

                )

        );
    }


    /*
     change Checked Exception(JsonProcessingException) to Unchecked (IllegalStateException),
     so that @Transaction and rollback will work on it
     */
    private String toJson(Object event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException ex) {
            throw new EventSerializationException("Failed to serialize outgoing inventory event", ex);
        }
    }
}
