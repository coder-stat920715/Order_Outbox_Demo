package com.example.outbox.order;

import com.example.outbox.config.KafkaConstants;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * *** DELIBERATELY BUGGY - the Dual Write Problem ***
 *
 * PostgreSQL and Kafka cannot share a transaction, so one of them can always succeed while the other fails:
 *
 *  - Kafka send BEFORE commit: Kafka gets the event, then the commit fails -> phantom event for an order that does not exist.
 *  - Kafka send AFTER commit (this class): the order is durable, then Kafka is unreachable -> the event is lost forever.
 *
 * This class models the second variant, which is what "send after commit" (e.g. @TransactionalEventListener(AFTER_COMMIT))
 * gives you. An exception thrown from afterCommit() reaches the caller but can no longer roll back the DB.
 */
@Service
public class NaiveOrderService {

    private static final Logger log = LoggerFactory.getLogger(NaiveOrderService.class);

    private final OrderRepository orderRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public NaiveOrderService(OrderRepository orderRepository, KafkaTemplate<String, String> kafkaTemplate,
                             ObjectMapper objectMapper) {
        this.orderRepository = orderRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public Order createOrder(CreateOrderRequest request, boolean simulateKafkaFailure) {
        Order order = orderRepository.save(Order.from(request));
        log.info("[NAIVE] Order {} saved, transaction about to commit", order.getId());

        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                log.info("[NAIVE] Order {} COMMITTED to PostgreSQL. Now publishing to Kafka...", order.getId());
                publish(order, simulateKafkaFailure);
            }
        });
        return order;
    }

    private void publish(Order order, boolean simulateKafkaFailure) {
        if (simulateKafkaFailure) {
            log.error("[NAIVE] Simulated Kafka outage for order {} - event will be LOST", order.getId());
            throw new EventPublishException(order.getId(), "Simulated Kafka outage", null);
        }
        try {
            String json = objectMapper.writeValueAsString(OrderCreatedEvent.from(order));
            var record = new ProducerRecord<>(KafkaConstants.ORDER_CREATED_TOPIC, order.getId().toString(), json);
            record.headers().add(KafkaConstants.EVENT_ID_HEADER,
                    UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
            kafkaTemplate.send(record).get(10, TimeUnit.SECONDS);
            log.info("[NAIVE] Event for order {} published", order.getId());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new EventPublishException(order.getId(), "Interrupted while publishing", e);
        } catch (JsonProcessingException | ExecutionException | TimeoutException | RuntimeException e) {
            log.error("[NAIVE] Kafka publish FAILED for order {} AFTER DB commit - event lost forever: {}",
                    order.getId(), e.toString());
            throw new EventPublishException(order.getId(), "Kafka publish failed after DB commit", e);
        }
    }
}
