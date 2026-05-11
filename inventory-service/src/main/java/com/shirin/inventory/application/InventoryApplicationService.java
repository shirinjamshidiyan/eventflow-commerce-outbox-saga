package com.shirin.inventory.application;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.shirin.inventory.domain.InventoryItem;
import com.shirin.inventory.domain.InventoryItemRepository;
import com.shirin.inventory.idempotency.ProcessedEvent;
import com.shirin.inventory.idempotency.ProcessedEventRepository;
import com.shirin.inventory.messaging.events.InventoryReservationFailedEvent;
import com.shirin.inventory.messaging.events.InventoryReservedEvent;
import com.shirin.inventory.messaging.events.OrderCreatedEvent;
import com.shirin.inventory.outbox.OutboxEvent;
import com.shirin.inventory.outbox.OutboxEventRepository;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.UUID;

@Service
@AllArgsConstructor
public class InventoryApplicationService {

    private final ProcessedEventRepository idempotencyRepository;
    private final InventoryItemRepository inventoryRepository;
    private final OutboxEventRepository outboxRepository;
    private final ObjectMapper objectMapper;

    @Transactional
   public void ProcessOrderCreatedEvent(OrderCreatedEvent event) throws JsonProcessingException {

        if(event== null || event.eventId()==null)
        {
            // fail
        }

        if(idempotencyRepository.existsById(event.eventId())) {  //todo: redis
            return; //idempotent event
        }

        boolean reserved = true;
        String failureReason = null;

        for (var item: event.items())
        {
          InventoryItem inventoryItem = inventoryRepository
                  .findById(item.skuId())
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
       }
      if(reserved)
      {
          for (var item : event.items()) {
              InventoryItem inventoryItem = inventoryRepository
                      .findById(item.skuId())
                      .orElseThrow();

              inventoryItem.decrease(item.quantity()); //todo: lock??
          }

        InventoryReservedEvent successEvent = new InventoryReservedEvent(
                UUID.randomUUID(),
                event.orderId()
        );
        outboxRepository.save(OutboxEvent.createPendingEvent(
                successEvent.eventId(),
                "Inventory",
                event.orderId(),
                "InventoryReserved",
                objectMapper.writeValueAsString(successEvent)
         ));
      } else {
        InventoryReservationFailedEvent failureEvent  = new InventoryReservationFailedEvent(
                UUID.randomUUID(), event.orderId(), failureReason);

        outboxRepository.save(OutboxEvent.createPendingEvent(
                failureEvent.eventId(),
                "Inventory",
                event.orderId(),
                "InventoryReservationFailed",
                objectMapper.writeValueAsString(failureEvent)
        ));
    }
//save in idempotency table
    idempotencyRepository.save(new ProcessedEvent(event.eventId()));
            //todo : event.eventId() is true????


    }
}
