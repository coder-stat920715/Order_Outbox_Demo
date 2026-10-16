package com.example.outbox.inventory;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "processed_events")
public class ProcessedEvent {

    @Id
    @Column(name = "event_id")
    private UUID eventId;

    @Column(nullable = false)
    private String consumer;

    @Column(name = "processed_at", nullable = false)
    private Instant processedAt;

    protected ProcessedEvent() {}

    public UUID getEventId() { return eventId; }
    public String getConsumer() { return consumer; }
    public Instant getProcessedAt() { return processedAt; }
}
