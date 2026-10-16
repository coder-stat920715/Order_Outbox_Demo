package com.example.outbox.outbox;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import java.time.Duration;

@ConfigurationProperties(prefix = "app.outbox")
public record OutboxProperties(
        @DefaultValue("50") int batchSize,
        @DefaultValue("10") int maxRetries,
        @DefaultValue("10s") Duration ackTimeout) {}
