package com.shirin.inventory.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.UUID;

@Entity
@Table(name = "inventory_items")
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

    public void decrease(int requestedQuantity)  //todo1: multi-instance condition => Lock
    {
       if(!hasEnoughQuantity(requestedQuantity))
       {
           throw new IllegalStateException("Not enough inventory"); //todo: error handling
       }
       this.availableQuantity -= requestedQuantity;

    }
}
