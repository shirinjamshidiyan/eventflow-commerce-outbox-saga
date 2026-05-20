package com.shirin.inventory.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;

import java.util.UUID;

@Entity
@Table(name = "inventory_items")
@Getter
public class InventoryItem {

    @Id
    @Column(name = "sku_id")
    private  UUID skuId;

    @Column(name = "available_quantity", nullable = false)
    private  int availableQuantity;

    protected InventoryItem(){}
    public boolean hasEnoughQuantity(int requestedQuantity)
    {
        return this.availableQuantity >= requestedQuantity;
    }

    public void decrease(int requestedQuantity)
    {
        if (requestedQuantity <= 0) {
            throw new IllegalArgumentException("requestedQuantity must be positive");
        }

        if(!hasEnoughQuantity(requestedQuantity))
       {
           throw new IllegalStateException("Not enough inventory");
       }
       this.availableQuantity -= requestedQuantity;

    }

    public void increase(int quantity) {
        if (quantity <= 0) {
            throw new IllegalArgumentException("Quantity must be positive");
        }
        this.availableQuantity += quantity;
    }
}
