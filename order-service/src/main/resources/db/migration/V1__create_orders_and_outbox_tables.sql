CREATE TABLE orders (
            id UUID PRIMARY KEY,
            status VARCHAR(50) NOT NULL CHECK (
                status IN ('PENDING_INVENTORY', 'INVENTORY_RESERVED', 'CANCELLED')),
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE order_items (
             id UUID PRIMARY KEY,
             order_id UUID NOT NULL,
             sku_id UUID NOT NULL,
             quantity INT NOT NULL CHECK (quantity > 0),

             CONSTRAINT fk_order_items_orders
                 FOREIGN KEY (order_id)
                     REFERENCES orders(id)
);

CREATE TABLE outbox_events (
           id UUID PRIMARY KEY,
           aggregate_type VARCHAR(100) NOT NULL,
           aggregate_id UUID NOT NULL,
           event_type VARCHAR(100) NOT NULL,
           payload TEXT NOT NULL,
           status VARCHAR(50) NOT NULL CHECK (
               status IN ('PENDING', 'PUBLISHED', 'FAILED', 'DEAD')),
           created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
           published_at TIMESTAMPTZ,
           last_error TEXT,
           retry_count INT NOT NULL DEFAULT 0,
           next_retry_at TIMESTAMPTZ
);
