# Testing Guide: Dual Write Bug vs Transactional Outbox

Estimated time: 20 minutes. Everything runs locally.

## 0. Prerequisites
* Docker + Docker Compose v2, JDK 21, Maven 3.9+, Postman (desktop or web agent).
* Ports free: 5432 (Postgres), 9092 (Kafka), 8080 (app), 8089 (Kafka UI).

## 1. One-time setup

### 1.1 Start infrastructure
```bash
docker compose up -d
docker compose ps          # postgres and kafka should become "healthy" (~20-30s)
```

### 1.2 Start the application (keep this terminal visible: you will read its logs)
```bash
mvn spring-boot:run
```
Expected: Flyway applies `V1__init_schema.sql`, topic `outbox.order-created` is created, and the app listens on 8080.
Idle logs are quiet, since the relayer logs only when it finds PENDING rows.

### 1.3 Import Postman files
Postman > Import > select both files from `postman/`:
* `outbox-demo.postman_collection.json`
* `outbox-demo.postman_environment.json`  -> then select environment **Outbox Demo - Local** (top right).

### 1.4 Open three helper windows
| Purpose | Command / URL |
|---|---|
| SQL shell | `docker compose exec postgres psql -U app orders` |
| Kafka UI | http://localhost:8089 > Topics > `outbox.order-created` > Messages |
| Kafka CLI (optional) | `docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic outbox.order-created --from-beginning --property print.key=true --property print.headers=true --timeout-ms 5000` |

### 1.5 Reset state between runs (optional, in psql)
```sql
TRUNCATE orders, outbox_events, processed_events;
UPDATE inventory_stock SET available = 100, reserved = 0;
```
(Kafka topic content is not cleared; filter by the orderId key instead.)

Useful queries used below:
```sql
-- A: orders
SELECT id, sku, quantity, status, created_at FROM orders ORDER BY created_at DESC LIMIT 5;
-- B: outbox
SELECT id, aggregate_id AS order_id, status, retry_count, created_at, updated_at
FROM outbox_events ORDER BY created_at DESC LIMIT 5;
-- C: inventory + dedup
SELECT * FROM inventory_stock;
SELECT event_id, consumer, processed_at FROM processed_events ORDER BY processed_at DESC LIMIT 5;
```

---

## Scenario A: Sanity check (Postman folder "0 - Sanity")
1. Send **0.1 Validation error**.
2. Expected: `400`, test passes. Nothing appears in the DB, no logs from services.

## Scenario B: Reproduce the Dual Write bug, simulated (folder "1")
Goal: prove the order exists but the event is lost.

1. Send **1.1 Naive - happy path (Kafka UP)**.
   * Expected: `201`.
   * App log: `[NAIVE] Order <id> saved...` > `COMMITTED to PostgreSQL. Now publishing...` > `Event for order <id> published` > `[INVENTORY] Received event ...` > `Reserved 2 x SKU-RED-SHOE`.
   * Kafka UI: a message with key = `naiveHappyOrderId`.
   > Everything works when Kafka is healthy, which is why this bug is easy to miss.
2. Send **1.2 Naive - simulated failure after DB commit** (`?simulateKafkaFailure=true`).
   * Expected: `503`, body message `DUAL WRITE FAILURE: order is committed in PostgreSQL but the Kafka event was lost`.
   * App log: `COMMITTED to PostgreSQL` > `Simulated Kafka outage ... event will be LOST` > `Dual write failure: order <id> exists in DB...`.
3. Prove the inconsistency (use the id in Postman console variable `naiveLostOrderId`):
   ```sql
   SELECT * FROM orders WHERE id = '<naiveLostOrderId>';          -- 1 row  (DB has it)
   SELECT * FROM outbox_events WHERE aggregate_id = '<naiveLostOrderId>';  -- 0 rows (nothing to retry)
   ```
   Kafka UI: no message with that key. Inventory `reserved` did not change.
   **Result:** the client was told "failed", the DB says "created", Kafka never heard, and no mechanism will ever fix it.

## Scenario C: Dual Write bug with a REAL Kafka outage
1. `docker compose pause kafka`
2. Send **1.3 [KAFKA PAUSED] Naive - real outage**.
   * Takes ~5-6 s (producer `max.block.ms` / `delivery.timeout.ms`), then `503`.
   * App log: `Kafka publish FAILED for order <id> AFTER DB commit - event lost forever: ...TimeoutException...`.
3. Check query A: the order exists. `docker compose unpause kafka`, wait 30 s: still nothing in Kafka for that key.

## Scenario D: Transactional Outbox, happy path (folder "2")
1. Make sure Kafka is running (`docker compose unpause kafka` if needed; `docker compose ps`).
2. Send **2.1 Outbox - happy path**. Expected `201` immediately.
3. App log sequence:
   ```
   [OUTBOX] Order <id> + outbox event <eid> written atomically (Kafka not involved)
   [RELAY] Claimed 1 pending outbox event(s)
   [RELAY] Event <eid> (aggregate <id>) ACKed by Kafka -> PUBLISHED
   [INVENTORY] Received event <eid> (key=<id>, partition=..., offset=...)
   [INVENTORY] Reserved 2 x SKU-BLUE-HAT for order <id> (event <eid>)
   ```
4. SQL: query B shows `PUBLISHED`, `retry_count` 0. Query C: `SKU-BLUE-HAT` reserved +2, and `processed_events` contains `<eid>`.
5. Kafka UI: message key = order id, header `event-id` = `<eid>`, header `event-type` = `ORDER_CREATED`.

## Scenario E: Transactional Outbox survives a Kafka outage (the key demo)
1. `docker compose pause kafka`
2. Send **2.2 [KAFKA PAUSED] Outbox - write path survives outage**.
   * Expected: `201` in well under 2 s. The request path never touches Kafka.
3. Immediately run query B: row is `PENDING`, `retry_count` 0 or 1.
4. Wait 20-30 s, rerun query B: `retry_count` increases (each attempt takes ~6 s because of the producer timeouts), status is still `PENDING`.
   App log: `[RELAY] Publish of event <eid> failed (attempt 2/10, status=PENDING): ...`.
   Order exists (query A), event is queued: **no inconsistency, nothing lost**.
5. `docker compose unpause kafka`.
6. Within ~10-30 s (producer/consumer reconnect):
   * `[RELAY] ... ACKed by Kafka -> PUBLISHED`, then `[INVENTORY] Reserved 3 x SKU-BLUE-HAT`.
   * Query B: `PUBLISHED`. Kafka UI shows exactly one message for the key.
7. Optional: keep Kafka paused longer than `retry_count` reaching 10 (about 60-70 s): the row moves to `FAILED`.
   Requeue it manually with:
   ```sql
   UPDATE outbox_events SET status = 'PENDING', retry_count = 0 WHERE status = 'FAILED';
   ```

## Scenario F: Idempotent consumer (duplicate delivery)
At-least-once delivery means duplicates are possible, for example the relayer crashing after the Kafka ACK and before the DB commit. Simulate by publishing the same `event-id` twice.

1. Note current reserved stock: `SELECT reserved FROM inventory_stock WHERE sku='SKU-GREEN-BAG';` (say N).
2. Send the same event twice (the two `\t` are real tab characters, which `printf` produces):
   ```bash
   EID=11111111-1111-1111-1111-111111111111
   LINE=$(printf "event-id:$EID\torder-dup-1\t{\"orderId\":\"7d1f5a0e-0000-4000-8000-000000000001\",\"customerId\":\"c9\",\"sku\":\"SKU-GREEN-BAG\",\"quantity\":2,\"totalAmount\":10.00,\"occurredAt\":\"2026-10-16T10:00:00Z\"}")
   for i in 1 2; do echo "$LINE" | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
     --bootstrap-server localhost:9092 --topic outbox.order-created \
     --property parse.headers=true --property parse.key=true; done
   ```
   (Alternative: Kafka UI > topic > Produce Message, key `order-dup-1`, header `event-id` = same UUID, same JSON value, press Produce twice.)
3. App log: first message `Reserved 2 x SKU-GREEN-BAG`, second message `Duplicate event 1111... - skipping (idempotent consumer)`.
4. SQL: `reserved` increased by 2 (N+2, not N+4); `processed_events` has exactly one row for that event id.

## Scenario G: Edge cases
* **2.3 unknown SKU:** `201`; Inventory logs `Cannot reserve ... (unknown SKU or insufficient stock)`; event is still marked processed, so no endless retry loop.
* **2.4 burst:** Postman > Runner > select only request 2.4, 20 iterations. Relayer logs batches (`Claimed N`). Check
  `SELECT status, count(*) FROM outbox_events GROUP BY status;` all end `PUBLISHED`.
* **SKIP LOCKED with two instances:** run `mvn spring-boot:run -Dspring-boot.run.arguments=--server.port=8081` in a second terminal, then rerun 2.4 with 50 iterations against 8080. Both instances' logs show `[RELAY] Claimed ...` for different rows; `SELECT count(*) FROM processed_events` equals the number of orders (no duplicates).

## Scenario H: Automated version
`mvn test` (Docker required) runs the same flows (Tests 1-3) with Testcontainers.

## Troubleshooting
| Symptom | Likely cause / fix |
|---|---|
| App fails at startup with Kafka connection warnings | Kafka not healthy yet: `docker compose ps`, wait, restart app. |
| `1.3` returns 503 instantly | Kafka paused/stopped before metadata fetch can still fail fast; fine. If it returns 201 Kafka was not really paused. |
| Outbox row never leaves PENDING after unpause | Wait up to ~30 s for reconnect; check app log for `[RELAY]` errors; ensure `retry_count` < 10 (otherwise it is FAILED, see Scenario E.7). |
| Port 8080/8089 in use | Change `server.port` or the Kafka UI mapping in `docker-compose.yml`. |
| Postman `503` shows as failure | Expected for 1.2/1.3; the test scripts assert 503 explicitly. |

## Cleanup
```bash
docker compose down -v
```
