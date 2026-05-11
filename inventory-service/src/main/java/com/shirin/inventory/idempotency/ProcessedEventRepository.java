package com.shirin.inventory.idempotency;

import com.shirin.inventory.domain.InventoryItem;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

//todo3: redis
public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, UUID> {
}
