package com.example.outbox.inventory;

import com.example.outbox.config.KafkaConstants;
import com.example.outbox.order.OrderCreatedEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

@Component
public class InventoryKafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(InventoryKafkaConsumer.class);

    private final InventoryService inventoryService;
    private final ObjectMapper objectMapper;

    public InventoryKafkaConsumer(InventoryService inventoryService, ObjectMapper objectMapper) {
        this.inventoryService = inventoryService;
        this.objectMapper = objectMapper;
    }

    /**
     * Offsets are committed only after this method returns, so a crash re-delivers the record (at-least-once).
     * Exceptions go to Spring Kafka's DefaultErrorHandler (bounded retries, then skipped + logged);
     * add a DeadLetterPublishingRecoverer for production.
     */
    @KafkaListener(topics = KafkaConstants.ORDER_CREATED_TOPIC)
    public void onOrderCreated(ConsumerRecord<String, String> record) throws JsonProcessingException {
        UUID eventId = eventId(record);
        log.info("[INVENTORY] Received event {} (key={}, partition={}, offset={})",
                eventId, record.key(), record.partition(), record.offset());
        inventoryService.reserveOnce(eventId, objectMapper.readValue(record.value(), OrderCreatedEvent.class));
    }

    private static UUID eventId(ConsumerRecord<String, String> record) {
        Header header = record.headers().lastHeader(KafkaConstants.EVENT_ID_HEADER);
        if (header == null) {
            throw new IllegalArgumentException("Missing '" + KafkaConstants.EVENT_ID_HEADER + "' header at offset "
                    + record.offset());
        }
        return UUID.fromString(new String(header.value(), StandardCharsets.UTF_8));
    }
}
