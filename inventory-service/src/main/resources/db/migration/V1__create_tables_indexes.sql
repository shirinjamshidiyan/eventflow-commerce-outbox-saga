CREATE TABLE inventory_items(
    sku_id UUID PRIMARY KEY ,
    available_quantity INT NOT NULL CHECK ( available_quantity >= 0 )
);

CREATE TABLE inventory_reservations (
    id UUID PRIMARY KEY,
    order_id UUID NOT NULL,
    sku_id UUID NOT NULL,
    quantity INT NOT NULL CHECK (quantity > 0),
    status VARCHAR(50) NOT NULL CHECK (
        status IN ('RESERVED', 'RELEASED')
        ),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    released_at TIMESTAMPTZ,

    CONSTRAINT uq_inventory_reservations_order_sku
        UNIQUE (order_id, sku_id)
);

CREATE INDEX idx_inventory_reservations_order_status
    ON inventory_reservations(order_id, status);

CREATE TABLE processed_events(
    event_id UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE outbox_events (
   id UUID PRIMARY KEY,
   aggregate_type VARCHAR(100) NOT NULL,
   aggregate_id UUID NOT NULL,
   event_type VARCHAR(100) NOT NULL,
   payload TEXT NOT NULL,
   status VARCHAR(50) NOT NULL CHECK (
       status IN ('PENDING', 'PROCESSING', 'PUBLISHED', 'FAILED', 'DEAD')),
   created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
   published_at TIMESTAMPTZ,
   published_by VARCHAR(100),
   last_error TEXT,
   retry_count INT NOT NULL DEFAULT 0,
   next_retry_at TIMESTAMPTZ,
   processing_started_at TIMESTAMPTZ,
   processing_by VARCHAR(100)
);

CREATE INDEX idx_inventory_outbox_publishable
    ON outbox_events(status, next_retry_at, created_at);

CREATE INDEX idx_inventory_outbox_processing
    ON outbox_events(status, processing_started_at);

INSERT INTO inventory_items (sku_id, available_quantity)
VALUES
    ('11111111-1111-1111-1111-111111111111', 10),
    ('11111111-1111-1111-2222-111111111111', 20);