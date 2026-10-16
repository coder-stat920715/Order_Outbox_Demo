package com.example.outbox.order;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "orders")
public class Order {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "customer_id", nullable = false)
    private String customerId;

    @Column(nullable = false)
    private String sku;

    @Column(nullable = false)
    private int quantity;

    @Column(name = "total_amount", nullable = false, precision = 12, scale = 2)
    private BigDecimal totalAmount;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private OrderStatus status;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    protected Order() {}

    public static Order from(CreateOrderRequest r) {
        Order o = new Order();
        o.customerId = r.customerId();
        o.sku = r.sku();
        o.quantity = r.quantity();
        o.totalAmount = r.totalAmount();
        o.status = OrderStatus.CREATED;
        o.createdAt = Instant.now();
        return o;
    }

    public UUID getId() { return id; }
    public String getCustomerId() { return customerId; }
    public String getSku() { return sku; }
    public int getQuantity() { return quantity; }
    public BigDecimal getTotalAmount() { return totalAmount; }
    public OrderStatus getStatus() { return status; }
    public Instant getCreatedAt() { return createdAt; }
}
