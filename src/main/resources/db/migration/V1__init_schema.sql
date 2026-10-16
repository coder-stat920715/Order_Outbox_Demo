-- ===== Order Service ==========================================================
CREATE TABLE orders (
    id            UUID           PRIMARY KEY,
    customer_id   VARCHAR(64)    NOT NULL,
    sku           VARCHAR(64)    NOT NULL,
    quantity      INT            NOT NULL CHECK (quantity > 0),
    total_amount  NUMERIC(12, 2) NOT NULL,
    status        VARCHAR(32)    NOT NULL,
    created_at    TIMESTAMPTZ    NOT NULL DEFAULT now()
);

-- ===== Transactional Outbox ===================================================
-- status is VARCHAR + CHECK instead of a native PG ENUM on purpose:
-- ALTER TYPE ... ADD VALUE is awkward inside transactional migrations.
CREATE TABLE outbox_events (
    id              UUID         PRIMARY KEY,
    aggregate_type  VARCHAR(64)  NOT NULL,
    aggregate_id    VARCHAR(64)  NOT NULL,
    event_type      VARCHAR(64)  NOT NULL,
    payload         JSONB        NOT NULL,
    status          VARCHAR(16)  NOT NULL DEFAULT 'PENDING'
                    CHECK (status IN ('PENDING', 'PUBLISHED', 'FAILED')),
    retry_count     INT          NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Partial index keeps the relayer's poll query cheap however large the table grows.
CREATE INDEX idx_outbox_pending ON outbox_events (created_at) WHERE status = 'PENDING';

-- ===== Inventory Service ======================================================
CREATE TABLE inventory_stock (
    sku        VARCHAR(64) PRIMARY KEY,
    available  INT NOT NULL CHECK (available >= 0),
    reserved   INT NOT NULL DEFAULT 0 CHECK (reserved >= 0)
);

INSERT INTO inventory_stock (sku, available) VALUES
    ('SKU-RED-SHOE', 100), ('SKU-BLUE-HAT', 100), ('SKU-GREEN-BAG', 100);

-- Idempotent-consumer deduplication table: the PK is what makes it safe.
CREATE TABLE processed_events (
    event_id      UUID         PRIMARY KEY,
    consumer      VARCHAR(64)  NOT NULL,
    processed_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);
