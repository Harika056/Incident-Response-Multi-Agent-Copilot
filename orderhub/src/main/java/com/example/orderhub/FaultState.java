package com.example.orderhub;

import java.util.concurrent.atomic.AtomicReference;

import org.springframework.stereotype.Component;

/** Holds the single fault currently injected into OrderHub (used by the eval harness). */
@Component
public class FaultState {

    public enum Fault { NONE, OUTAGE, KAFKA_LAG, SLOW_QUERY }

    private final AtomicReference<Fault> current = new AtomicReference<>(Fault.NONE);

    public Fault get() {
        return current.get();
    }

    public void set(Fault fault) {
        current.set(fault);
    }

    public boolean is(Fault fault) {
        return current.get() == fault;
    }
}
