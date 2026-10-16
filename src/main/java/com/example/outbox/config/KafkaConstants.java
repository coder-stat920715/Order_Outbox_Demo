package com.example.outbox.config;

public final class KafkaConstants {
    public static final String ORDER_CREATED_TOPIC = "outbox.order-created";
    /** Unique id of the event; consumers use it for deduplication. */
    public static final String EVENT_ID_HEADER = "event-id";
    public static final String EVENT_TYPE_HEADER = "event-type";

    private KafkaConstants() {}
}
