CREATE SEQUENCE order_number_seq
    START WITH 1
    INCREMENT BY 1;

CREATE TABLE orders (
    id UUID PRIMARY KEY,
    order_number VARCHAR(100) NOT NULL UNIQUE ,
    request_id UUID NOT NULL UNIQUE,
    checkout_id UUID NOT NULL,
    customer_id UUID NOT NULL,
    payment_method_id UUID NOT NULL,
    currency VARCHAR(3) NOT NULL,
    total_amount NUMERIC(12,2) NOT NULL CHECK ( total_amount >= 0 ),
    status VARCHAR(50) NOT NULL CHECK (
        status IN (
                   'INVENTORY_PENDING',
                   'INVENTORY_RESERVED',
                   'PAYMENT_PENDING',
                   'CONFIRMED',
                   'CANCELLATION_PENDING',
                   'CANCELLED'
        )
    ),
    cancellation_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ,
    confirmed_at TIMESTAMPTZ,
    cancelled_at TIMESTAMPTZ,
    version BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE order_items (
    id UUID PRIMARY KEY,
    order_id UUID NOT NULL,
    sku_id UUID NOT NULL,
    product_name VARCHAR(255) NOT NULL ,
    quantity INT NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(12, 2) NOT NULL CHECK ( unit_price >= 0 ),
    item_total_price NUMERIC(12, 2) NOT NULL CHECK (item_total_price >= 0),
     CONSTRAINT fk_order_items_orders
         FOREIGN KEY (order_id)
             REFERENCES orders(id)
);

CREATE TABLE processed_events(
    event_id UUID PRIMARY KEY ,
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

CREATE INDEX idx_outbox_publishable
    ON outbox_events(status, next_retry_at, created_at);

CREATE INDEX idx_outbox_processing
    ON outbox_events(status, processing_started_at);