package com.example.outbox.order;

import java.util.UUID;

/** Thrown when Kafka publication fails AFTER the database transaction has already committed. */
public class EventPublishException extends RuntimeException {
    private final UUID orderId;

    public EventPublishException(UUID orderId, String message, Throwable cause) {
        super(message, cause);
        this.orderId = orderId;
    }

    public UUID orderId() { return orderId; }
}
