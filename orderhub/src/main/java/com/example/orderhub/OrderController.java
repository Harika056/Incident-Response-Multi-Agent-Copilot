package com.example.orderhub;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class OrderController {

    private static final Logger log = LoggerFactory.getLogger(OrderController.class);

    public record NewOrder(String sku, Integer qty) {}

    private final JdbcTemplate jdbc;
    private final KafkaTemplate<String, String> kafka;
    private final FaultState faults;

    public OrderController(JdbcTemplate jdbc, KafkaTemplate<String, String> kafka, FaultState faults) {
        this.jdbc = jdbc;
        this.kafka = kafka;
        this.faults = faults;
    }

    @PostMapping("/orders")
    public ResponseEntity<Map<String, Object>> create(@RequestBody NewOrder body) {
        if (faults.is(FaultState.Fault.OUTAGE)) {
            log.error("Order persistence failed: unable to acquire JDBC connection (HikariPool-1 - Connection is not available, request timed out after 30000ms)");
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(Map.of("error", "service unavailable"));
        }
        String sku = body.sku() == null ? "SKU-1" : body.sku();
        int qty = body.qty() == null ? 1 : body.qty();
        Long id = jdbc.queryForObject(
                "INSERT INTO orders(sku, qty, status) VALUES (?, ?, 'NEW') RETURNING id", Long.class, sku, qty);
        kafka.send("orders.created", sku, "{\"orderId\":" + id + ",\"sku\":\"" + sku + "\",\"qty\":" + qty + "}");
        log.info("Order accepted id={} sku={} qty={}", id, sku, qty);
        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of("orderId", id));
    }

    @GetMapping("/orders/recent")
    public ResponseEntity<Object> recent() {
        if (faults.is(FaultState.Fault.OUTAGE)) {
            log.error("Failed to load recent orders: HikariPool-1 - Connection is not available, request timed out after 30000ms");
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(Map.of("error", "service unavailable"));
        }
        long started = System.currentTimeMillis();
        List<Map<String, Object>> rows;
        if (faults.is(FaultState.Fault.SLOW_QUERY)) {
            // Injected fault: an expensive statement (pg_sleep stands in for a bad plan / missing index).
            rows = jdbc.queryForList(
                    "SELECT o.id, o.sku, o.qty, o.status FROM orders o, (SELECT pg_sleep(4)) s ORDER BY o.created_at DESC LIMIT 20");
        } else {
            rows = jdbc.queryForList("SELECT id, sku, qty, status FROM orders ORDER BY created_at DESC LIMIT 20");
        }
        long took = System.currentTimeMillis() - started;
        if (took > 1000) {
            log.warn("Slow database call while loading recent orders: {} ms", took);
        }
        return ResponseEntity.ok(rows);
    }
}
