package com.example.outbox;

import com.example.outbox.config.KafkaConstants;
import com.example.outbox.order.CreateOrderRequest;
import com.example.outbox.order.OrderCreatedEvent;
import com.example.outbox.order.OrderRepository;
import com.example.outbox.order.OrderResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.DockerClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Requires Docker. "Kafka down" is simulated with `docker pause` on the broker container: the mapped port stays
 * the same (unlike stop/start), connections hang exactly like a network partition, and unpause = recovery.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "app.outbox.poll-interval-ms=500")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Disabled
class OutboxPatternIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(OutboxPatternIntegrationTest.class);

    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");
    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.0"));

    static {
        Startables.deepStart(POSTGRES, KAFKA).join();
    }

    @DynamicPropertySource
    static void containerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @Autowired TestRestTemplate rest;
    @Autowired JdbcTemplate jdbc;
    @Autowired OrderRepository orderRepository;
    @Autowired KafkaTemplate<String, String> kafkaTemplate;
    @Autowired ObjectMapper objectMapper;

    // ------------------------------------------------------------------------------------------------------------
    // Test 1: the Dual Write bug
    // ------------------------------------------------------------------------------------------------------------
    @Test
    @Order(1)
    void naiveEndpoint_losesEventWhenKafkaIsDown() {
        var request = new CreateOrderRequest("cust-naive", "SKU-RED-SHOE", 3, new BigDecimal("59.97"));

        pauseKafka();
        ResponseEntity<OrderResponse> response;
        try {
            response = rest.postForEntity("/api/v1/orders/naive", request, OrderResponse.class);
        } finally {
            unpauseKafka();
        }

        // The caller sees an error...
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        UUID orderId = response.getBody().orderId();

        // ...but the order IS in PostgreSQL (the DB commit already happened).
        assertThat(orderRepository.findById(orderId)).isPresent();

        // ...and the event is gone: nothing in Kafka, nothing in any outbox, nothing will ever retry it.
        assertThat(outboxRowCount(orderId)).isZero();
        assertThat(consumeFor(orderId.toString(), Duration.ofSeconds(8)))
                .as("Kafka must NOT contain an event for order %s (dual write bug)", orderId)
                .isEmpty();
        assertThat(reservedStock("SKU-RED-SHOE")).as("inventory never learned about the order").isZero();
        log.info("TEST 1 OK: order {} exists in PostgreSQL, but its event was lost", orderId);
    }

    // ------------------------------------------------------------------------------------------------------------
    // Test 2: the Transactional Outbox survives a Kafka outage
    // ------------------------------------------------------------------------------------------------------------
    @Test
    @Order(2)
    void outboxEndpoint_survivesKafkaOutage_andDeliversAfterRecovery() {
        var request = new CreateOrderRequest("cust-outbox", "SKU-BLUE-HAT", 3, new BigDecimal("29.97"));

        pauseKafka();
        UUID orderId;
        try {
            ResponseEntity<OrderResponse> response =
                    rest.postForEntity("/api/v1/orders/outbox", request, OrderResponse.class);

            // The write path does not depend on Kafka at all.
            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
            orderId = response.getBody().orderId();
            assertThat(orderRepository.findById(orderId)).isPresent();
            assertThat(outboxRowCount(orderId)).isEqualTo(1);

            // While Kafka is down the relayer keeps retrying; the row must stay PENDING (not lost, not FAILED).
            await().atMost(Duration.ofSeconds(45)).untilAsserted(() ->
                    assertThat(retryCount(response.getBody().orderId())).isGreaterThanOrEqualTo(1));
            assertThat(outboxStatus(orderId)).isEqualTo("PENDING");
            log.info("Relayer retried {} time(s) while Kafka was down; event still PENDING", retryCount(orderId));
        } finally {
            unpauseKafka();
        }

        // Kafka is back: the relayer must deliver without any manual intervention.
        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(500)).untilAsserted(() ->
                assertThat(outboxStatus(orderId)).isEqualTo("PUBLISHED"));

        UUID outboxId = jdbc.queryForObject("SELECT id FROM outbox_events WHERE aggregate_id = ?", UUID.class,
                orderId.toString());

        List<ConsumerRecord<String, String>> received = consumeFor(orderId.toString(), Duration.ofSeconds(8));
        assertThat(received).hasSize(1);
        assertThat(received.get(0).value()).contains(orderId.toString());
        assertThat(new String(received.get(0).headers().lastHeader(KafkaConstants.EVENT_ID_HEADER).value(),
                StandardCharsets.UTF_8)).isEqualTo(outboxId.toString());

        // End-to-end: the Inventory consumer processed it exactly once and reserved the stock.
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(processedCount(outboxId)).isEqualTo(1);
            assertThat(reservedStock("SKU-BLUE-HAT")).isEqualTo(3);
        });
        log.info("TEST 2 OK: event {} survived the outage and was delivered + processed", outboxId);
    }

    // ------------------------------------------------------------------------------------------------------------
    // Test 3 (bonus): at-least-once delivery => the consumer must tolerate duplicates
    // ------------------------------------------------------------------------------------------------------------
    @Test
    @Order(3)
    void inventoryConsumer_isIdempotent_whenSameEventIsDeliveredTwice() throws Exception {
        UUID eventId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        String payload = objectMapper.writeValueAsString(new OrderCreatedEvent(
                orderId, "cust-dup", "SKU-GREEN-BAG", 2, new BigDecimal("10.00"), Instant.now()));

        for (int i = 0; i < 2; i++) { // same event-id twice = what a relayer retry after a lost ACK produces
            var record = new ProducerRecord<>(KafkaConstants.ORDER_CREATED_TOPIC, orderId.toString(), payload);
            record.headers().add(KafkaConstants.EVENT_ID_HEADER, eventId.toString().getBytes(StandardCharsets.UTF_8));
            kafkaTemplate.send(record).get();
        }

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(reservedStock("SKU-GREEN-BAG")).isEqualTo(2));
        Thread.sleep(3_000); // give the duplicate time to be (wrongly) applied
        assertThat(reservedStock("SKU-GREEN-BAG")).as("stock reserved exactly once").isEqualTo(2);
        assertThat(processedCount(eventId)).isEqualTo(1);
    }

    // ----------------------------------------------------------------------------------------------- helpers
    private static void pauseKafka() {
        log.warn(">>> PAUSING Kafka container (simulated outage)");
        dockerClient().pauseContainerCmd(KAFKA.getContainerId()).exec();
    }

    private static void unpauseKafka() {
        log.warn(">>> UNPAUSING Kafka container (recovery)");
        dockerClient().unpauseContainerCmd(KAFKA.getContainerId()).exec();
    }

    private static DockerClient dockerClient() { return DockerClientFactory.lazyClient(); }

    private int outboxRowCount(UUID orderId) {
        return jdbc.queryForObject("SELECT count(*) FROM outbox_events WHERE aggregate_id = ?", Integer.class,
                orderId.toString());
    }

    private String outboxStatus(UUID orderId) {
        return jdbc.queryForObject("SELECT status FROM outbox_events WHERE aggregate_id = ?", String.class,
                orderId.toString());
    }

    private int retryCount(UUID orderId) {
        return jdbc.queryForObject("SELECT retry_count FROM outbox_events WHERE aggregate_id = ?", Integer.class,
                orderId.toString());
    }

    private int processedCount(UUID eventId) {
        return jdbc.queryForObject("SELECT count(*) FROM processed_events WHERE event_id = ?", Integer.class, eventId);
    }

    private int reservedStock(String sku) {
        return jdbc.queryForObject("SELECT reserved FROM inventory_stock WHERE sku = ?", Integer.class, sku);
    }

    /** Independent probe consumer (own group, from earliest) that collects records for one key during a time window. */
    private List<ConsumerRecord<String, String>> consumeFor(String key, Duration window) {
        Map<String, Object> config = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "probe-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        List<ConsumerRecord<String, String>> matches = new ArrayList<>();
        try (var consumer = new KafkaConsumer<String, String>(config)) {
            consumer.subscribe(List.of(KafkaConstants.ORDER_CREATED_TOPIC));
            long deadline = System.nanoTime() + window.toNanos();
            while (System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(500)).forEach(r -> {
                    if (key.equals(r.key())) matches.add(r);
                });
            }
        }
        return matches;
    }
}
