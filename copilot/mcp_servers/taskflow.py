"""MCP server: least-privilege TaskFlow ticketing (stdio).

Run standalone:  python -m copilot.mcp_servers.taskflow

Least privilege is enforced in three layers:
  1. TaskFlow (Spring Security) only honours the JWT's `roles` claim, so the real
     enforcement is server-side.
  2. The copilot's token carries only TICKET_READ / TICKET_CREATE / TICKET_COMMENT. It does
     not carry TICKET_DELETE, so even a hand-crafted DELETE call would get a 403.
  3. This MCP server exposes no delete (and no update/close) tool at all.
"""
from __future__ import annotations

from typing import Any

import httpx
import jwt
from mcp.server.mcpserver import MCPServer

from ..config import Settings, get_settings

REQUIRED_ROLES = {"TICKET_READ", "TICKET_CREATE", "TICKET_COMMENT"}
FORBIDDEN_ROLES = {"TICKET_DELETE", "TICKET_ADMIN"}
SEVERITIES = ("SEV1", "SEV2", "SEV3", "SEV4")

mcp = MCPServer("taskflow-tickets")


def token_roles(token: str) -> set[str]:
    """Fail-fast, client-side read of the roles claim. NOT a security check (signature is
    verified by TaskFlow); it just gives the agent a clear error and refuses over-privileged tokens."""
    try:
        claims = jwt.decode(token, options={"verify_signature": False})
    except jwt.PyJWTError as exc:
        raise PermissionError(f"TASKFLOW_JWT is not a valid JWT: {exc}") from exc
    return set(claims.get("roles", []))


def assert_least_privilege(token: str) -> None:
    if not token:
        raise PermissionError("TASKFLOW_JWT is not set")
    roles = token_roles(token)
    over = roles & FORBIDDEN_ROLES
    if over:
        raise PermissionError(f"refusing to run with over-privileged token (has {sorted(over)})")
    missing = REQUIRED_ROLES - roles
    if missing:
        raise PermissionError(f"token is missing required roles {sorted(missing)}")


class TaskflowClient:
    def __init__(self, settings: Settings | None = None, client: httpx.Client | None = None):
        self.settings = settings or get_settings()
        self._client = client or httpx.Client(timeout=10)

    def _headers(self) -> dict[str, str]:
        assert_least_privilege(self.settings.taskflow_jwt)
        return {"Authorization": f"Bearer {self.settings.taskflow_jwt}"}

    def _call(self, method: str, path: str, **kw) -> dict[str, Any]:
        try:
            resp = self._client.request(method, self.settings.taskflow_url + path, headers=self._headers(), **kw)
        except PermissionError as exc:
            return {"error": str(exc)}
        except httpx.HTTPError as exc:
            return {"error": f"{type(exc).__name__}: {exc}"}
        if resp.status_code >= 400:
            return {"error": f"TaskFlow returned HTTP {resp.status_code}", "detail": resp.text[:300]}
        return resp.json()

    def create_ticket(self, title: str, description: str, severity: str, incident_id: str = "") -> dict[str, Any]:
        if severity not in SEVERITIES:
            return {"error": f"severity must be one of {SEVERITIES}"}
        return self._call(
            "POST",
            "/api/tickets",
            json={"title": title, "description": description, "severity": severity, "incidentId": incident_id},
        )

    def get_ticket(self, ticket_id: str) -> dict[str, Any]:
        return self._call("GET", f"/api/tickets/{ticket_id}")

    def list_tickets(self) -> dict[str, Any]:
        result = self._call("GET", "/api/tickets")
        return result if "error" in result else {"tickets": result}

    def add_comment(self, ticket_id: str, body: str) -> dict[str, Any]:
        return self._call("POST", f"/api/tickets/{ticket_id}/comments", json={"body": body})


_client: TaskflowClient | None = None


def _tf() -> TaskflowClient:
    global _client
    if _client is None:
        _client = TaskflowClient()
    return _client


@mcp.tool()
def create_ticket(title: str, description: str, severity: str, incident_id: str = "") -> dict[str, Any]:
    """File an incident ticket in TaskFlow. severity is one of SEV1..SEV4."""
    return _tf().create_ticket(title, description, severity, incident_id)


@mcp.tool()
def get_ticket(ticket_id: str) -> dict[str, Any]:
    """Fetch a ticket by id."""
    return _tf().get_ticket(ticket_id)


@mcp.tool()
def list_tickets() -> dict[str, Any]:
    """List existing tickets (use to avoid filing duplicates)."""
    return _tf().list_tickets()


@mcp.tool()
def add_comment(ticket_id: str, body: str) -> dict[str, Any]:
    """Append a comment to a ticket (e.g. remediation outcome)."""
    return _tf().add_comment(ticket_id, body)


if __name__ == "__main__":
    mcp.run("stdio")
