import datetime as dt

import httpx

from copilot import diagnostics as d
from copilot.config import Settings

NOW = dt.datetime(2026, 11, 24, 12, 0, tzinfo=dt.timezone.utc)

LOG = "\n".join([
    "2026-11-24T11:00:00.000+00:00|ERROR|c.e.orderhub.Old|stale error outside the window",
    "2026-11-24T11:58:00.000+00:00|INFO|c.e.o.OrderController|Order accepted id=1 sku=SKU-1 qty=1",
    "2026-11-24T11:59:00.000+00:00|ERROR|c.e.o.DependencyMonitor|HikariPool-1 - Connection is not available",
    "garbage line without separators",
    "2026-11-24T11:59:30.000+00:00|WARN|c.e.o.OrderController|Slow database call while loading recent orders: 4012 ms",
]) + "\n"


def test_parse_log_line():
    assert d.parse_log_line("a|ERROR|log|msg with | pipe") == {
        "ts": "a", "level": "ERROR", "logger": "log", "msg": "msg with | pipe"}
    assert d.parse_log_line("nonsense") is None


def test_search_logs_window_level_and_query(tmp_path):
    p = tmp_path / "app.log"
    p.write_text(LOG)
    res = d.search_logs(str(p), minutes=10, now=NOW)
    assert res["level_counts_in_window"] == {"INFO": 1, "ERROR": 1, "WARN": 1}   # stale line excluded
    assert [e["level"] for e in d.search_logs(str(p), level="error", minutes=10, now=NOW)["entries"]] == ["ERROR"]
    slow = d.search_logs(str(p), query="slow database", minutes=10, now=NOW)
    assert slow["matched"] == 1 and "4012" in slow["entries"][0]["msg"]


def test_search_logs_missing_file(tmp_path):
    assert "error" in d.search_logs(str(tmp_path / "nope.log"))


def test_compute_lag():
    res = d.compute_lag({0: 10, 1: None}, {0: 25, 1: 7, 2: 0})
    assert res["total_lag"] == 15 + 7
    assert {p["partition"]: p["lag"] for p in res["partitions"]} == {0: 15, 1: 7, 2: 0}


def test_probe_endpoint_allowlist_and_unreachable():
    s = Settings(orderhub_url="http://orderhub")
    assert "not allowed" in d.probe_endpoint(s, "/admin/faults")["error"]     # cannot peek at fault state
    assert "not allowed" in d.probe_endpoint(s, "/orders")["error"]

    def boom(request):
        raise httpx.ConnectError("refused")

    res = d.probe_endpoint(s, "/actuator/health", httpx.Client(transport=httpx.MockTransport(boom)))
    assert res["reachable"] is False

    ok = httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(503, json={"status": "DOWN"})))
    res = d.probe_endpoint(s, "/actuator/health", ok)
    assert res["reachable"] is True and res["http_status"] == 503


async def test_diag_server_exposes_only_read_tools():
    from copilot.mcp_servers import orderhub_diag
    names = {t.name for t in await orderhub_diag.mcp.list_tools()}
    assert names == {"probe_endpoint", "search_logs", "get_kafka_consumer_lag",
                     "get_active_queries", "get_table_stats", "run_readonly_sql"}
    assert not any(w in n for n in names for w in ("restart", "reset", "resume", "cancel", "remediate", "write"))


async def test_run_readonly_sql_rejects_before_touching_db():
    from copilot.mcp_servers import orderhub_diag
    res = orderhub_diag.run_readonly_sql("DROP TABLE orders")
    assert "query rejected" in res["error"]
