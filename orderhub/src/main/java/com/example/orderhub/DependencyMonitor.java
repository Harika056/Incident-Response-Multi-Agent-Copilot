package com.example.orderhub;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Health indicator + periodic dependency check. While the OUTAGE fault is active it reports DOWN
 * and logs repeating connection errors. Names/messages deliberately avoid mentioning "fault" or
 * "outage" so the logs do not leak the injected ground truth to the diagnostic agents.
 */
@Component("dependency")
public class DependencyMonitor implements HealthIndicator {

    private static final Logger log = LoggerFactory.getLogger(DependencyMonitor.class);
    private final FaultState faults;

    public DependencyMonitor(FaultState faults) {
        this.faults = faults;
    }

    @Override
    public Health health() {
        if (faults.is(FaultState.Fault.OUTAGE)) {
            return Health.down().withDetail("error", "connection pool exhausted").build();
        }
        return Health.up().build();
    }

    @Scheduled(fixedDelay = 2000)
    public void checkDependencies() {
        if (faults.is(FaultState.Fault.OUTAGE)) {
            log.error("HikariPool-1 - Connection is not available, request timed out after 30000ms");
            log.error("Health check failing: downstream dependency unavailable");
        }
    }
}
