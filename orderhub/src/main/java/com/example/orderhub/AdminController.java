package com.example.orderhub;

import java.util.Locale;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Token-protected control plane. Two audiences, neither of which is the diagnostic agents:
 *  - /admin/faults/*      -> the eval harness (fault injection)
 *  - /admin/remediate/*   -> the human-approved remediation executor
 * The diagnostic MCP server has no admin token and cannot call these endpoints.
 */
@RestController
@RequestMapping("/admin")
public class AdminController {

    private static final Logger log = LoggerFactory.getLogger(AdminController.class);

    private final FaultState faults;
    private final KafkaListenerEndpointRegistry registry;
    private final JdbcTemplate jdbc;
    private final String adminToken;

    public AdminController(FaultState faults, KafkaListenerEndpointRegistry registry, JdbcTemplate jdbc,
                           @Value("${orderhub.admin-token}") String adminToken) {
        this.faults = faults;
        this.registry = registry;
        this.jdbc = jdbc;
        this.adminToken = adminToken;
    }

    private boolean unauthorized(String token) {
        return token == null || !token.equals(adminToken);
    }

    private MessageListenerContainer consumer() {
        return registry.getListenerContainer(InventoryConsumer.LISTENER_ID);
    }

    @GetMapping("/faults")
    public ResponseEntity<Map<String, Object>> current(@RequestHeader(value = "X-Admin-Token", required = false) String token) {
        if (unauthorized(token)) return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        return ResponseEntity.ok(Map.of("fault", faults.get().name()));
    }

    /** type: none | outage | kafka_lag | slow_query */
    @PostMapping("/faults/{type}")
    public ResponseEntity<Map<String, Object>> inject(@PathVariable String type,
            @RequestHeader(value = "X-Admin-Token", required = false) String token) {
        if (unauthorized(token)) return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        FaultState.Fault fault;
        try {
            fault = FaultState.Fault.valueOf(type.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", "unknown fault " + type));
        }
        clearEffects();
        faults.set(fault);
        if (fault == FaultState.Fault.KAFKA_LAG) {
            consumer().pause();
        }
        return ResponseEntity.ok(Map.of("fault", fault.name()));
    }

    /** restart_service | resume_consumer | cancel_slow_queries */
    @PostMapping("/remediate/{action}")
    public ResponseEntity<Map<String, Object>> remediate(@PathVariable String action,
            @RequestHeader(value = "X-Admin-Token", required = false) String token) {
        if (unauthorized(token)) return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        FaultState.Fault target;
        switch (action) {
            case "restart_service" -> target = FaultState.Fault.OUTAGE;
            case "resume_consumer" -> target = FaultState.Fault.KAFKA_LAG;
            case "cancel_slow_queries" -> target = FaultState.Fault.SLOW_QUERY;
            default -> {
                return ResponseEntity.badRequest().body(Map.of("error", "unknown action " + action));
            }
        }
        boolean fixed = faults.is(target);
        if (action.equals("cancel_slow_queries")) {
            jdbc.queryForList("SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                    + "WHERE datname = current_database() AND pid <> pg_backend_pid() AND query LIKE '%pg_sleep%'");
        }
        if (fixed) {
            clearEffects();
            faults.set(FaultState.Fault.NONE);
        }
        log.info("Operator action '{}' executed", action);
        return ResponseEntity.ok(Map.of("action", action, "fixed", fixed, "fault", faults.get().name()));
    }

    private void clearEffects() {
        MessageListenerContainer c = consumer();
        if (c != null && c.isContainerPaused()) {
            c.resume();
        } else if (c != null && c.isPauseRequested()) {
            c.resume();
        }
    }
}
