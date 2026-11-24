"""MCP server: read-only OrderHub diagnostics (stdio).

Run standalone:  python -m copilot.mcp_servers.orderhub_diag

Exposes ONLY read-only tools. There is deliberately no restart / reset / write tool here;
remediation goes through the human-approved executor (copilot/remediation.py).
"""
from __future__ import annotations

from typing import Any

from mcp.server.mcpserver import MCPServer

from .. import diagnostics as d
from ..config import get_settings
from ..guard import UnsafeSQL

mcp = MCPServer("orderhub-diagnostics")


def _safe(fn, *args, **kwargs) -> dict[str, Any]:
    """Tool errors become data the agent can reason about instead of crashing the run."""
    try:
        return fn(*args, **kwargs)
    except UnsafeSQL as exc:
        return {"error": f"query rejected: {exc}"}
    except Exception as exc:  # noqa: BLE001
        return {"error": f"{type(exc).__name__}: {exc}"}


@mcp.tool()
def probe_endpoint(path: str) -> dict[str, Any]:
    """GET a read-only OrderHub endpoint and report reachability, HTTP status and latency.
    Allowed paths: /actuator/health, /orders/recent."""
    return _safe(d.probe_endpoint, get_settings(), path)


@mcp.tool()
def search_logs(query: str = "", level: str = "", minutes: int = 30, limit: int = 40) -> dict[str, Any]:
    """Search recent OrderHub application logs. `level` is optional (ERROR, WARN, INFO).
    `query` is a case-insensitive substring. Returns level counts for the window plus the
    most recent matching entries."""
    s = get_settings()
    return _safe(d.search_logs, s.orderhub_log_path, query, level, minutes, limit)


@mcp.tool()
def get_kafka_consumer_lag() -> dict[str, Any]:
    """Committed offset, log-end offset and lag per partition for OrderHub's consumer group,
    plus the consumer group's state and member count."""
    return _safe(d.kafka_consumer_lag, get_settings())


@mcp.tool()
def get_active_queries(min_duration_ms: int = 1000) -> dict[str, Any]:
    """Currently running Postgres queries (from pg_stat_activity) that have run at least
    `min_duration_ms`, longest first."""
    return _safe(d.active_queries, get_settings(), min_duration_ms)


@mcp.tool()
def get_table_stats() -> dict[str, Any]:
    """Row estimates and scan counters for OrderHub tables."""
    return _safe(d.table_stats, get_settings())


@mcp.tool()
def run_readonly_sql(sql: str) -> dict[str, Any]:
    """Run ONE read-only SELECT/WITH statement against the OrderHub database (read-only
    role, read-only transaction, statement timeout). Writes are rejected."""
    return _safe(d.run_readonly_sql, get_settings(), sql)


if __name__ == "__main__":
    mcp.run("stdio")
