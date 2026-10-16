package com.example.outbox.outbox;

import com.example.outbox.config.KafkaConstants;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Polling relayer. One poll = one DB transaction:
 *   1. SELECT ... FOR UPDATE SKIP LOCKED  (claim rows; other instances skip them)
 *   2. send to Kafka, wait for broker ACK (acks=all)
 *   3. mark PUBLISHED (or bump retry_count) and COMMIT, which also releases the locks
 *
 * Guarantees AT-LEAST-ONCE: if the app crashes after the Kafka ACK but before the commit, the row is re-sent.
 * Consumers must therefore be idempotent (see InventoryService).
 *
 * Trade-off: a DB connection is held while waiting for Kafka (bounded by the producer's delivery.timeout.ms).
 * That is what keeps the row locks valid; keep batches modest. Log-tailing CDC (Debezium) avoids polling altogether.
 */
@Component
public class OutboxPublisherJob {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisherJob.class);

    private record Inflight(OutboxEvent event, CompletableFuture<SendResult<String, String>> ack) {}

    private final OutboxEventRepository repository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final OutboxProperties props;

    public OutboxPublisherJob(OutboxEventRepository repository, KafkaTemplate<String, String> kafkaTemplate,
                              OutboxProperties props) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
        this.props = props;
    }

    @Scheduled(fixedDelayString = "${app.outbox.poll-interval-ms:1000}") // fixedDelay: never overlaps itself
    @Transactional
    public void publishPendingEvents() {
        List<OutboxEvent> batch = repository.lockNextBatch(props.batchSize());
        if (batch.isEmpty()) {
            return;
        }
        log.info("[RELAY] Claimed {} pending outbox event(s)", batch.size());

        // Fire all sends first (pipelined), then wait for the ACKs.
        List<Inflight> inflight = new ArrayList<>();
        for (OutboxEvent event : batch) {
            try {
                inflight.add(new Inflight(event, kafkaTemplate.send(toRecord(event))));
            } catch (RuntimeException e) {
                onFailure(event, e);
                break; // broker unreachable: stop early, leave the rest PENDING for the next poll
            }
        }

        for (Inflight f : inflight) {
            try {
                f.ack().get(props.ackTimeout().toMillis(), TimeUnit.MILLISECONDS);
                f.event().markPublished();
                log.info("[RELAY] Event {} (aggregate {}) ACKed by Kafka -> PUBLISHED",
                        f.event().getId(), f.event().getAggregateId());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("[RELAY] Interrupted while awaiting ACKs; unprocessed rows stay PENDING");
                return;
            } catch (ExecutionException | TimeoutException e) {
                onFailure(f.event(), e);
            }
        }
    }

    private ProducerRecord<String, String> toRecord(OutboxEvent e) {
        var record = new ProducerRecord<>(KafkaConstants.ORDER_CREATED_TOPIC, e.getAggregateId(), e.getPayload());
        record.headers()
                .add(KafkaConstants.EVENT_ID_HEADER, e.getId().toString().getBytes(StandardCharsets.UTF_8))
                .add(KafkaConstants.EVENT_TYPE_HEADER, e.getEventType().getBytes(StandardCharsets.UTF_8));
        return record;
    }

    private void onFailure(OutboxEvent event, Exception cause) {
        event.recordFailure(props.maxRetries());
        log.warn("[RELAY] Publish of event {} failed (attempt {}/{}, status={}): {}",
                event.getId(), event.getRetryCount(), props.maxRetries(), event.getStatus(), cause.toString());
    }
}
