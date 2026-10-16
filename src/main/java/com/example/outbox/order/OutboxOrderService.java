package com.example.outbox.order;

import com.example.outbox.outbox.OutboxEvent;
import com.example.outbox.outbox.OutboxEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * *** CORRECT: Transactional Outbox ***
 * The order row and the outbox row are written in ONE local PostgreSQL transaction: both commit or neither does.
 * Kafka is not touched here at all - it is the relayer's job ({@code OutboxPublisherJob}).
 */
@Service
public class OutboxOrderService {

    private static final Logger log = LoggerFactory.getLogger(OutboxOrderService.class);

    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxRepository;
    private final ObjectMapper objectMapper;

    public OutboxOrderService(OrderRepository orderRepository, OutboxEventRepository outboxRepository,
                              ObjectMapper objectMapper) {
        this.orderRepository = orderRepository;
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public Order createOrder(CreateOrderRequest request) {
        Order order = orderRepository.save(Order.from(request));

        String payload;
        try {
            payload = objectMapper.writeValueAsString(OrderCreatedEvent.from(order));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot serialize OrderCreatedEvent", e); // rolls back the order too
        }
        OutboxEvent event = outboxRepository.save(
                OutboxEvent.pending("ORDER", order.getId().toString(), "ORDER_CREATED", payload));

        log.info("[OUTBOX] Order {} + outbox event {} written atomically (Kafka not involved)",
                order.getId(), event.getId());
        return order;
    }
}
