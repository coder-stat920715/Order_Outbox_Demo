package com.example.outbox.inventory;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import java.util.UUID;

public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, UUID> {

    /**
     * Atomic "claim" of an event id. Returns 1 if this call inserted it (first delivery), 0 if it already existed
     * (duplicate). Race-safe: a concurrent insert of the same id blocks on the PK until the other tx finishes.
     */
    @Modifying
    @Query(value = """
            INSERT INTO processed_events (event_id, consumer, processed_at)
            VALUES (:eventId, :consumer, now())
            ON CONFLICT (event_id) DO NOTHING
            """, nativeQuery = true)
    int insertIfAbsent(@Param("eventId") UUID eventId, @Param("consumer") String consumer);
}
