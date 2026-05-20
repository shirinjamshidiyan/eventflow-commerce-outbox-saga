package com.shirin.inventory.domain;

import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.UUID;

public interface InventoryReservationRepository extends JpaRepository<InventoryReservation, UUID> {

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("""
            select r
            from InventoryReservation r
            where r.orderId = :orderId
              and r.status = :status
            order by r.skuId asc
            """)
    List<InventoryReservation> findAllByOrderIdAndStatusForUpdate(
            @Param("orderId") UUID orderId,
            @Param("status") InventoryReservationStatus status
    );

}
