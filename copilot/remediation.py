"""Remediation executor - the ONLY code path that changes OrderHub.

It is never exposed to an agent as a tool. It is called by the human-facing CLI after
``Store.decide(..., approve=True)``. ``Store.claim_for_execution`` re-checks the approval
atomically, so a proposal cannot run unapproved or run twice.
"""
from __future__ import annotations

import httpx

from .config import Settings, get_settings
from .store import ACTIONS, Store


def execute_proposal(
    store: Store,
    proposal_id: str,
    settings: Settings | None = None,
    client: httpx.Client | None = None,
) -> dict:
    settings = settings or get_settings()
    proposal = store.claim_for_execution(proposal_id)  # raises ApprovalRequired if not approved
    action = proposal["action"]
    if action not in ACTIONS:  # defensive; the store already validates
        store.finish_execution(proposal_id, False, f"unknown action {action}")
        raise ValueError(f"unknown action {action}")

    owns_client = client is None
    client = client or httpx.Client(timeout=15)
    try:
        resp = client.post(
            f"{settings.orderhub_url}/admin/remediate/{action}",
            headers={"X-Admin-Token": settings.orderhub_admin_token},
        )
        ok = resp.status_code == 200
        body = resp.text[:500]
    except httpx.HTTPError as exc:
        ok, body = False, f"request failed: {exc}"
    finally:
        if owns_client:
            client.close()

    store.finish_execution(proposal_id, ok, body)
    return {"proposal_id": proposal_id, "action": action, "ok": ok, "result": body}
