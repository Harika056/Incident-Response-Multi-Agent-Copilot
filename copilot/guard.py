"""Guard rails for agent-supplied SQL.

This is defence in depth: the diagnostic MCP server also connects as a read-only
Postgres role, opens a READ ONLY transaction and sets a statement_timeout.
"""
from __future__ import annotations

import re

_FORBIDDEN = re.compile(
    r"\b(insert|update|delete|drop|alter|truncate|create|grant|revoke|copy|call|do|"
    r"vacuum|reindex|cluster|lock|listen|notify|set|reset|refresh|merge|"
    r"pg_terminate_backend|pg_cancel_backend|pg_sleep|pg_read_file|lo_import|lo_export|dblink)\b",
    re.IGNORECASE,
)


class UnsafeSQL(ValueError):
    pass


def _strip_comments(sql: str) -> str:
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    return re.sub(r"--[^\n]*", " ", sql)


def validate_readonly_sql(sql: str) -> str:
    """Return the cleaned single SELECT/WITH statement or raise UnsafeSQL."""
    cleaned = _strip_comments(sql).strip().rstrip(";").strip()
    if not cleaned:
        raise UnsafeSQL("empty statement")
    if ";" in cleaned:
        raise UnsafeSQL("multiple statements are not allowed")
    if not re.match(r"^(select|with)\b", cleaned, re.IGNORECASE):
        raise UnsafeSQL("only SELECT / WITH queries are allowed")
    match = _FORBIDDEN.search(cleaned)
    if match:
        raise UnsafeSQL(f"forbidden keyword: {match.group(1).lower()}")
    return cleaned
