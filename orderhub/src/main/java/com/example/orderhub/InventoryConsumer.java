package com.example.orderhub;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/** Consumes orders.created and decrements inventory. Paused by the KAFKA_LAG fault. */
@Component
public class InventoryConsumer {

    public static final String LISTENER_ID = "inventory-consumer";
    private static final Logger log = LoggerFactory.getLogger(InventoryConsumer.class);

    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper = new ObjectMapper();

    public InventoryConsumer(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @KafkaListener(id = LISTENER_ID, topics = "orders.created", groupId = "orderhub-inventory")
    public void onOrder(String payload) throws Exception {
        JsonNode node = mapper.readTree(payload);
        String sku = node.get("sku").asText();
        int qty = node.get("qty").asInt();
        jdbc.update("UPDATE inventory SET stock = stock - ? WHERE sku = ?", qty, sku);
        jdbc.update("UPDATE orders SET status = 'RESERVED' WHERE id = ?", node.get("orderId").asLong());
        log.debug("Inventory reserved for order {}", node.get("orderId").asLong());
    }
}
