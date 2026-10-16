# Dual Write Problem vs. Transactional Outbox (Spring Boot 3.4 / Java 21)

One Spring Boot app containing both the **Order Service** (`order`, `outbox` packages) and the
**Inventory Service** (`inventory` package) to keep the demo runnable with a single process.
Split along package boundaries into two deployables when you are ready.

```
POST /api/v1/orders/naive   -> Order -> PostgreSQL (commit) -> KafkaTemplate.send   [BUGGY dual write]
POST /api/v1/orders/outbox  -> Order + outbox_events (ONE tx) -> OutboxPublisherJob -> Kafka -> InventoryKafkaConsumer
                                                              (SKIP LOCKED poll)       (processed_events dedup)
```

## Run the automated proof (needs Docker)
    mvn test

* Test 1 - `docker pause` Kafka, call `/naive`: HTTP 503, order is in PostgreSQL, **no** event in Kafka, stock never reserved.
* Test 2 - `docker pause` Kafka, call `/outbox`: HTTP 201, row stays PENDING while `retry_count` climbs; unpause -> PUBLISHED,
  event received exactly once on the topic, Inventory reserves stock.
* Test 3 - same event-id delivered twice: stock reserved once.

## Run it by hand
    docker compose up -d          # Postgres :5432, Kafka :9092, Kafka UI http://localhost:8089
    mvn spring-boot:run

    # Phase 1: dual write bug
    curl -s -XPOST 'localhost:8080/api/v1/orders/naive?simulateKafkaFailure=true' -H 'Content-Type: application/json' \
      -d '{"customerId":"c1","sku":"SKU-RED-SHOE","quantity":2,"totalAmount":19.98}'
    docker exec -it $(docker compose ps -q postgres) psql -U app orders -c "select id,status from orders"
    #   -> order exists; topic outbox.order-created (Kafka UI) has nothing for it. Lost forever.

    # Phase 2: outbox, with Kafka down
    docker compose pause kafka
    curl -s -XPOST localhost:8080/api/v1/orders/outbox -H 'Content-Type: application/json' \
      -d '{"customerId":"c1","sku":"SKU-BLUE-HAT","quantity":2,"totalAmount":19.98}'
    docker exec -it $(docker compose ps -q postgres) psql -U app orders -c "select status,retry_count from outbox_events"
    docker compose unpause kafka  # watch [RELAY] and [INVENTORY] logs: delivered + stock reserved

## Design notes
* `FOR UPDATE SKIP LOCKED` lets any number of app instances poll concurrently with no double-sends.
* Delivery is **at-least-once** (crash between Kafka ACK and DB commit => resend), hence `processed_events`.
  The dedup insert and the stock update share one transaction.
* Kafka message key = aggregate id (per-order ordering); the outbox row id travels as the `event-id` header.
* `status` is VARCHAR + CHECK rather than a native PG enum (painful to migrate).
* PUBLISHED rows are kept for audit; add a periodic delete/archive job for production volumes.
* For very high throughput, replace polling with log-based CDC (Debezium outbox event router).
