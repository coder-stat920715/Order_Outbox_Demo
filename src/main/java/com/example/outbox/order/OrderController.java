package com.example.outbox.order;

import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/orders")
public class OrderController {

    private static final Logger log = LoggerFactory.getLogger(OrderController.class);

    private final NaiveOrderService naiveService;
    private final OutboxOrderService outboxService;

    public OrderController(NaiveOrderService naiveService, OutboxOrderService outboxService) {
        this.naiveService = naiveService;
        this.outboxService = outboxService;
    }

    /** Phase 1 - buggy dual write. Add ?simulateKafkaFailure=true to fail right after the DB commit. */
    @PostMapping("/naive")
    public ResponseEntity<OrderResponse> createNaive(@Valid @RequestBody CreateOrderRequest request,
                                                     @RequestParam(defaultValue = "false") boolean simulateKafkaFailure) {
        try {
            Order order = naiveService.createOrder(request, simulateKafkaFailure);
            return ResponseEntity.status(HttpStatus.CREATED)
                    .body(new OrderResponse(order.getId(), order.getStatus().name(), "Order saved and event published"));
        } catch (EventPublishException e) {
            log.error("Dual write failure: order {} exists in DB but its event was never published", e.orderId());
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(new OrderResponse(
                    e.orderId(), OrderStatus.CREATED.name(),
                    "DUAL WRITE FAILURE: order is committed in PostgreSQL but the Kafka event was lost"));
        }
    }

    /** Phase 2 - transactional outbox. Succeeds even if Kafka is down; the relayer delivers later. */
    @PostMapping("/outbox")
    public ResponseEntity<OrderResponse> createOutbox(@Valid @RequestBody CreateOrderRequest request) {
        Order order = outboxService.createOrder(request);
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(new OrderResponse(order.getId(), order.getStatus().name(),
                        "Order saved; event queued in outbox for guaranteed delivery"));
    }
}
