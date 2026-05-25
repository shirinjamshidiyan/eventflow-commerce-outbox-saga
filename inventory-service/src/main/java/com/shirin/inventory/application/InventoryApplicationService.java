package com.shirin.inventory.application;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.shirin.contracts.common.EventEnvelope;
import com.shirin.contracts.common.EventSources;
import com.shirin.contracts.common.EventTypes;
import com.shirin.contracts.inventory.InventoryReleasedPayload;
import com.shirin.contracts.inventory.InventoryReservationFailedPayload;
import com.shirin.contracts.inventory.InventoryReservedPayload;
import com.shirin.contracts.order.InventoryReleaseRequestedPayload;
import com.shirin.contracts.order.OrderCreatedItemsPayload;
import com.shirin.contracts.order.OrderCreatedPayload;
import com.shirin.inventory.domain.*;
import com.shirin.inventory.idempotency.ProcessedEventRepository;
import com.shirin.inventory.outbox.OutboxEvent;
import com.shirin.inventory.outbox.OutboxEventRepository;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.util.*;
import java.util.stream.Collectors;

@Service
@AllArgsConstructor
@Slf4j
public class InventoryApplicationService {

    private final ProcessedEventRepository idempotencyRepository;
    private final InventoryItemRepository inventoryRepository;
    private final InventoryReservationRepository reservationRepository;
    private final OutboxEventRepository outboxRepository;
    private final ObjectMapper objectMapper;

    @Transactional
   public void processOrderCreatedEvent(EventEnvelope<OrderCreatedPayload> envelope)  {

        // idempotency check : processed_events insert
        int inserted = idempotencyRepository.insertIfAbsent(envelope.eventId());
        if (inserted == 0) {
            log.info("Duplicate order created event ignored");
            return;
        }


        // Group requested quantities by SKU and sum duplicate SKU entries
        Map<UUID, Integer> requestedBySku = envelope.payload().items().stream()
                .collect(Collectors.toMap(
                        OrderCreatedItemsPayload::skuId,
                        OrderCreatedItemsPayload::quantity,
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
                              envelope.payload().orderId(),
                              desiredSkuId,
                              desiredQuantity
                      )
              );

          }
          log.info("Inventory reserved for order");

          UUID eventId =  UUID.randomUUID();
          InventoryReservedPayload successPayload = new InventoryReservedPayload(envelope.payload().orderId());

          EventEnvelope<InventoryReservedPayload> newEnvelope = EventEnvelope.create(
                  eventId,
                  EventTypes.INVENTORY_RESERVED,
                  1,
                  envelope.correlationId(),
                  envelope.eventId(),
                  EventSources.INVENTORY_SERVICE,
                  successPayload
          );

          //  result outbox insert
        outboxRepository.save(
                OutboxEvent.createPendingEvent(
                        eventId,
                        "Inventory",
                        envelope.payload().orderId(),
                        EventTypes.INVENTORY_RESERVED,
                        toJson(newEnvelope)
         ));
          log.info("Inventory reserved event stored in outbox");
      } else
      {
          log.info("Inventory reservation failed");
          UUID eventId =  UUID.randomUUID();

          InventoryReservationFailedPayload failurePayload =
                  new InventoryReservationFailedPayload( envelope.payload().orderId(), failureReason);

          EventEnvelope<InventoryReservationFailedPayload> newEnvelope =
                  EventEnvelope.create(
                          eventId,
                          EventTypes.INVENTORY_RESERVATION_FAILED,
                          1,
                          envelope.correlationId(),
                          envelope.eventId(),
                          EventSources.INVENTORY_SERVICE,
                          failurePayload
                  );

          //  result outbox insert
         outboxRepository.save(
                OutboxEvent.createPendingEvent(
                        eventId,
                        "Inventory",
                        envelope.payload().orderId(),
                        EventTypes.INVENTORY_RESERVATION_FAILED,
                        toJson(newEnvelope)
        ));
          log.info("Inventory reservation failed event stored in outbox");
    }
   }

    @Transactional
    public void processInventoryReleaseRequestedEvent( EventEnvelope<InventoryReleaseRequestedPayload> envelope) {

        // idempotency check : processed_events insert
        int inserted = idempotencyRepository.insertIfAbsent(envelope.eventId());
        if (inserted == 0) {
            log.info("Duplicate inventory release requested event ignored");
            return;
        }

        List<InventoryReservation> sortedList = reservationRepository
                .findAllByOrderIdAndStatusForUpdate(
                        envelope.payload().orderId(),
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

        log.info("Inventory release completed, releasedReservations={}", sortedList.size());


        UUID eventId = UUID.randomUUID();

        InventoryReleasedPayload payload = new InventoryReleasedPayload(envelope.payload().orderId());

        EventEnvelope<InventoryReleasedPayload> newEnvelope =
                EventEnvelope.create(
                        eventId,
                        EventTypes.INVENTORY_RELEASED,
                        1,
                        envelope.correlationId(),
                        envelope.eventId(),
                        EventSources.INVENTORY_SERVICE,
                        payload
                );

        outboxRepository.save(
                OutboxEvent.createPendingEvent(
                        eventId,
                        "Inventory",
                        payload.orderId(),
                        EventTypes.INVENTORY_RELEASED,
                        toJson(newEnvelope)

                )
        );
        log.info("Inventory released event stored in outbox");
    }


    /*
     change Checked Exception(JsonProcessingException) to Unchecked (IllegalStateException),
     so that @Transaction and rollback will work on it
     */
    private String toJson(Object envelope) {
        try {
            return objectMapper.writeValueAsString(envelope);
        } catch (JsonProcessingException ex) {
            throw new EventSerializationException("Failed to serialize outgoing inventory envelope", ex);
        }
    }
}
