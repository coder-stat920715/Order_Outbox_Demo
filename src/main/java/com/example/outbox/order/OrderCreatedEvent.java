package com.example.outbox.order;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/** Integration event published to Kafka. The dedup id travels in the {@code event-id} header. */
public record OrderCreatedEvent(
        UUID orderId, String customerId, String sku, int quantity, BigDecimal totalAmount, Instant occurredAt) {

    public static OrderCreatedEvent from(Order o) {
        return new OrderCreatedEvent(o.getId(), o.getCustomerId(), o.getSku(), o.getQuantity(),
                o.getTotalAmount(), Instant.now());
    }
}
