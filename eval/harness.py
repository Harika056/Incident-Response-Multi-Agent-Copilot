"""Shared pieces of the evaluation harness: fault injection, background load, statistics."""
from __future__ import annotations

import math
import statistics
import threading
import time
from typing import Iterable

import httpx

from copilot.config import Settings

# Ground-truth label -> OrderHub admin fault name
FAULTS = {
    "SERVICE_OUTAGE": "outage",
    "KAFKA_CONSUMER_LAG": "kafka_lag",
    "SLOW_QUERY": "slow_query",
}

# Identical for every fault so the alert text carries no hint about the cause.
ALERT = "OrderHub order processing is degraded; customers report problems placing and fulfilling orders."


class FaultInjector:
    """Talks to OrderHub's token-protected admin API (the agents have no access to it)."""

    def __init__(self, settings: Settings, client: httpx.Client | None = None):
        self.settings = settings
        self.client = client or httpx.Client(timeout=15)

    def _post(self, fault: str) -> None:
        r = self.client.post(
            f"{self.settings.orderhub_url}/admin/faults/{fault}",
            headers={"X-Admin-Token": self.settings.orderhub_admin_token},
        )
        r.raise_for_status()

    def inject(self, label: str) -> None:
        self._post(FAULTS[label])

    def clear(self) -> None:
        self._post("none")

    def current(self) -> str:
        r = self.client.get(
            f"{self.settings.orderhub_url}/admin/faults",
            headers={"X-Admin-Token": self.settings.orderhub_admin_token},
        )
        r.raise_for_status()
        return r.json()["fault"]

    def warmup(self, n: int = 30) -> None:
        """Create some traffic so the consumer group has committed offsets before any fault."""
        for i in range(n):
            self.client.post(f"{self.settings.orderhub_url}/orders", json={"sku": f"SKU-{i % 3 + 1}", "qty": 1})
        time.sleep(3)


class LoadGenerator:
    """Steady background traffic: order placement plus periodic reads of recent orders."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []

    def _orders(self) -> None:
        with httpx.Client(timeout=10) as c:
            i = 0
            while not self._stop.is_set():
                try:
                    c.post(f"{self.settings.orderhub_url}/orders", json={"sku": f"SKU-{i % 3 + 1}", "qty": 1})
                except httpx.HTTPError:
                    pass
                i += 1
                self._stop.wait(0.2)

    def _reads(self) -> None:
        with httpx.Client(timeout=15) as c:
            while not self._stop.is_set():
                try:
                    c.get(f"{self.settings.orderhub_url}/orders/recent")
                except httpx.HTTPError:
                    pass
                self._stop.wait(1.0)

    def __enter__(self) -> "LoadGenerator":
        for fn in (self._orders, self._reads):
            t = threading.Thread(target=fn, daemon=True)
            t.start()
            self._threads.append(t)
        return self

    def __exit__(self, *exc) -> None:
        self._stop.set()
        for t in self._threads:
            t.join(timeout=20)


# ------------------------------------------------------------------------ statistics
def wilson_interval(successes: int, n: int, z: float = 1.96) -> tuple[float, float]:
    """95% Wilson score interval for a proportion (honest about small N)."""
    if n == 0:
        return (0.0, 0.0)
    p = successes / n
    denom = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / denom
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denom
    return (max(0.0, centre - half), min(1.0, centre + half))


def percentile(values: Iterable[float], q: float) -> float:
    vals = sorted(values)
    if not vals:
        return float("nan")
    k = (len(vals) - 1) * q
    lo, hi = math.floor(k), math.ceil(k)
    return vals[lo] + (vals[hi] - vals[lo]) * (k - lo)


def median(values: Iterable[float]) -> float:
    vals = list(values)
    return statistics.median(vals) if vals else float("nan")
