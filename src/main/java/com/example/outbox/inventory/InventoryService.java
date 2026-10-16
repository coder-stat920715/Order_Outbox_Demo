package com.example.outbox.inventory;

import com.example.outbox.order.OrderCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
public class InventoryService {

    private static final Logger log = LoggerFactory.getLogger(InventoryService.class);
    static final String CONSUMER = "inventory-service";

    private final ProcessedEventRepository processedEvents;
    private final JdbcClient jdbc;

    public InventoryService(ProcessedEventRepository processedEvents, JdbcClient jdbc) {
        this.processedEvents = processedEvents;
        this.jdbc = jdbc;
    }

    /**
     * Dedup marker + business effect share ONE transaction: either both persist or neither does,
     * so a crash mid-way can never lose the marker or reserve twice.
     */
    @Transactional
    public void reserveOnce(UUID eventId, OrderCreatedEvent event) {
        if (processedEvents.insertIfAbsent(eventId, CONSUMER) == 0) {
            log.info("[INVENTORY] Duplicate event {} for order {} - skipping (idempotent consumer)",
                    eventId, event.orderId());
            return;
        }

        int updated = jdbc.sql("""
                        UPDATE inventory_stock
                           SET available = available - :qty, reserved = reserved + :qty
                         WHERE sku = :sku AND available >= :qty
                        """)
                .param("qty", event.quantity())
                .param("sku", event.sku())
                .update();

        if (updated == 1) {
            log.info("[INVENTORY] Reserved {} x {} for order {} (event {})",
                    event.quantity(), event.sku(), event.orderId(), eventId);
        } else {
            // Business rejection, not a technical failure: do not retry forever. Real systems emit a StockRejected event.
            log.warn("[INVENTORY] Cannot reserve {} x {} for order {} (unknown SKU or insufficient stock)",
                    event.quantity(), event.sku(), event.orderId());
        }
    }
}
