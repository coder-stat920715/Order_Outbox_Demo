package com.example.outbox.order;

import java.util.UUID;

public record OrderResponse(UUID orderId, String status, String message) {}
