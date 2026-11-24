"""Read-only diagnostic primitives for OrderHub (logs, health, Kafka, Postgres).

Pure functions with injectable dependencies so they are unit-testable without any
infrastructure. ``mcp_servers/orderhub_diag.py`` wraps them as MCP tools.
Nothing in this module mutates OrderHub.
"""
from __future__ import annotations

import datetime as dt
import decimal
import os
import time
from collections import deque
from typing import Any

import httpx

from .config import Settings
from .guard import validate_readonly_sql

# Only these read-only GET paths may be probed.
PROBE_ALLOWLIST = ("/actuator/health", "/orders/recent")


def jsonable(value: Any) -> Any:
    if isinstance(value, decimal.Decimal):
        return float(value)
    if isinstance(value, (dt.datetime, dt.date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [jsonable(v) for v in value]
    return value


# --------------------------------------------------------------------------- logs
def parse_log_line(line: str) -> dict[str, str] | None:
    """Parse ``ts|LEVEL|logger|message`` (see orderhub/src/main/resources/application.yml)."""
    parts = line.rstrip("\n").split("|", 3)
    if len(parts) != 4:
        return None
    return {"ts": parts[0], "level": parts[1].strip(), "logger": parts[2].strip(), "msg": parts[3]}


def _parse_ts(ts: str) -> dt.datetime | None:
    try:
        parsed = dt.datetime.fromisoformat(ts)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=dt.timezone.utc)


def search_logs(
    path: str,
    query: str = "",
    level: str = "",
    minutes: int = 30,
    limit: int = 40,
    tail_lines: int = 5000,
    now: dt.datetime | None = None,
) -> dict[str, Any]:
    if not os.path.exists(path):
        return {"error": f"log file not found: {path}", "entries": []}
    now = now or dt.datetime.now(dt.timezone.utc)
    cutoff = now - dt.timedelta(minutes=max(1, minutes))
    level = level.upper().strip()
    needle = query.lower().strip()

    with open(path, "r", errors="replace") as fh:
        lines = deque(fh, maxlen=tail_lines)

    matches: list[dict[str, str]] = []
    level_counts: dict[str, int] = {}
    for raw in lines:
        entry = parse_log_line(raw)
        if entry is None:
            continue
        ts = _parse_ts(entry["ts"])
        if ts is not None and ts < cutoff:
            continue
        level_counts[entry["level"]] = level_counts.get(entry["level"], 0) + 1
        if level and entry["level"] != level:
            continue
        if needle and needle not in (entry["msg"] + " " + entry["logger"]).lower():
            continue
        matches.append(entry)

    return {
        "window_minutes": minutes,
        "level_counts_in_window": level_counts,
        "matched": len(matches),
        "entries": matches[-limit:],  # most recent
    }


# ------------------------------------------------------------------------- health
def probe_endpoint(settings: Settings, path: str, client: httpx.Client | None = None) -> dict[str, Any]:
    if path not in PROBE_ALLOWLIST:
        return {"error": f"path not allowed; choose one of {PROBE_ALLOWLIST}"}
    owns = client is None
    client = client or httpx.Client(timeout=8)
    started = time.perf_counter()
    try:
        resp = client.get(settings.orderhub_url + path)
        return {
            "path": path,
            "reachable": True,
            "http_status": resp.status_code,
            "latency_ms": round((time.perf_counter() - started) * 1000),
            "body_excerpt": resp.text[:300],
        }
    except httpx.HTTPError as exc:
        return {
            "path": path,
            "reachable": False,
            "latency_ms": round((time.perf_counter() - started) * 1000),
            "error": f"{type(exc).__name__}: {exc}",
        }
    finally:
        if owns:
            client.close()


# -------------------------------------------------------------------------- kafka
def compute_lag(committed: dict[int, int | None], end: dict[int, int]) -> dict[str, Any]:
    """committed/end are partition -> offset. A missing commit counts as offset 0."""
    partitions = []
    total = 0
    for p in sorted(end):
        c = committed.get(p)
        lag = max(0, end[p] - (c or 0))
        total += lag
        partitions.append({"partition": p, "committed": c, "log_end": end[p], "lag": lag})
    return {"total_lag": total, "partitions": partitions}


def kafka_consumer_lag(settings: Settings) -> dict[str, Any]:
    from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition  # imported lazily

    admin = KafkaAdminClient(bootstrap_servers=settings.kafka_bootstrap, request_timeout_ms=8000)
    consumer = KafkaConsumer(bootstrap_servers=settings.kafka_bootstrap, request_timeout_ms=8000)
    try:
        parts = consumer.partitions_for_topic(settings.kafka_topic) or set()
        tps = [TopicPartition(settings.kafka_topic, p) for p in sorted(parts)]
        end = {tp.partition: off for tp, off in consumer.end_offsets(tps).items()} if tps else {}

        raw = admin.list_group_offsets(settings.kafka_group)
        by_tp = raw.get(settings.kafka_group, raw) if isinstance(raw, dict) else {}
        committed = {
            tp.partition: getattr(meta, "offset", meta)
            for tp, meta in by_tp.items()
            if tp.topic == settings.kafka_topic
        }

        group_info: dict[str, Any] = {}
        try:
            described = admin.describe_groups([settings.kafka_group])
            info = described.get(settings.kafka_group, described) if isinstance(described, dict) else {}
            group_info = {
                "state": info.get("state") or info.get("group_state"),
                "member_count": len(info.get("members", []) or []),
            }
        except Exception as exc:  # describe is best-effort
            group_info = {"describe_error": f"{type(exc).__name__}: {exc}"}

        result = compute_lag(committed, end)
        result.update({"topic": settings.kafka_topic, "group": settings.kafka_group, "group_info": group_info})
        return result
    finally:
        consumer.close()
        admin.close()


# ---------------------------------------------------------------------- postgres
def _pg_connect(settings: Settings):
    import psycopg
    from psycopg.rows import dict_row

    return psycopg.connect(
        settings.pg_dsn_ro,
        connect_timeout=5,
        row_factory=dict_row,
        options=f"-c statement_timeout={settings.pg_statement_timeout_ms} -c default_transaction_read_only=on",
    )


def _pg_query(settings: Settings, sql: str, params: tuple = ()) -> list[dict[str, Any]]:
    with _pg_connect(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return [jsonable(r) for r in cur.fetchall()]


def active_queries(settings: Settings, min_duration_ms: int = 1000) -> dict[str, Any]:
    rows = _pg_query(
        settings,
        """
        SELECT pid, state, wait_event_type, wait_event,
               (EXTRACT(EPOCH FROM (now() - query_start)) * 1000)::bigint AS running_ms,
               left(query, 300) AS query
        FROM pg_stat_activity
        WHERE datname = current_database()
          AND pid <> pg_backend_pid()
          AND state <> 'idle'
          AND (EXTRACT(EPOCH FROM (now() - query_start)) * 1000) >= %s
        ORDER BY running_ms DESC
        LIMIT 20
        """,
        (min_duration_ms,),
    )
    return {"min_duration_ms": min_duration_ms, "count": len(rows), "queries": rows}


def table_stats(settings: Settings) -> dict[str, Any]:
    rows = _pg_query(
        settings,
        "SELECT relname AS table_name, n_live_tup, seq_scan, idx_scan, n_dead_tup "
        "FROM pg_stat_user_tables ORDER BY relname",
    )
    return {"tables": rows}


def run_readonly_sql(settings: Settings, sql: str, max_rows: int = 50) -> dict[str, Any]:
    cleaned = validate_readonly_sql(sql)  # raises UnsafeSQL
    rows = _pg_query(settings, cleaned)
    return {"row_count": len(rows), "rows": rows[:max_rows], "truncated": len(rows) > max_rows}
