"""Durable checkpoint + approval store (SQLite).

Two tables:

* ``incidents``  - one row per investigation: ADK session id, status, final diagnosis.
* ``proposals``  - remediation proposals. A proposal is created by the agent as ``pending``
  and can only become ``approved`` through :meth:`Store.decide`, which is called by the
  human-facing CLI. The agents are never given a tool that can do that.

Because the state lives on disk (and the ADK session in its own SQLite DB), a run can
pause for approval, the process can exit, and a later process can resume the same session.
"""
from __future__ import annotations

import json
import os
import sqlite3
import time
import uuid
from contextlib import contextmanager
from typing import Any, Iterator

ROOT_CAUSES = ("SERVICE_OUTAGE", "KAFKA_CONSUMER_LAG", "SLOW_QUERY", "OTHER")
ACTIONS = ("restart_service", "resume_consumer", "cancel_slow_queries")

# Which remediation is the right one for each root cause (used by the eval harness).
EXPECTED_ACTION = {
    "SERVICE_OUTAGE": "restart_service",
    "KAFKA_CONSUMER_LAG": "resume_consumer",
    "SLOW_QUERY": "cancel_slow_queries",
}

_SCHEMA = """
CREATE TABLE IF NOT EXISTS incidents (
  id TEXT PRIMARY KEY,
  created_at REAL NOT NULL,
  alert TEXT NOT NULL,
  session_id TEXT NOT NULL,
  status TEXT NOT NULL,            -- investigating | awaiting_approval | resolved | closed
  root_cause TEXT,
  confidence REAL,
  evidence TEXT,                   -- JSON list
  recommended_action TEXT,
  diagnosed_at REAL
);
CREATE TABLE IF NOT EXISTS proposals (
  id TEXT PRIMARY KEY,
  incident_id TEXT NOT NULL REFERENCES incidents(id),
  action TEXT NOT NULL,
  rationale TEXT NOT NULL,
  status TEXT NOT NULL,            -- pending | approved | rejected | executed | failed
  created_at REAL NOT NULL,
  decided_at REAL,
  decided_by TEXT,
  result TEXT
);
"""


class ApprovalRequired(PermissionError):
    """Raised when something tries to execute a remediation that a human has not approved."""


class Store:
    def __init__(self, path: str):
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self.path = path
        with self._conn() as c:
            c.executescript(_SCHEMA)

    @contextmanager
    def _conn(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path, timeout=30)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    # ---- incidents ---------------------------------------------------------
    def create_incident(self, alert: str, session_id: str) -> str:
        incident_id = "INC-" + uuid.uuid4().hex[:8]
        with self._conn() as c:
            c.execute(
                "INSERT INTO incidents(id, created_at, alert, session_id, status) VALUES (?,?,?,?,?)",
                (incident_id, time.time(), alert, session_id, "investigating"),
            )
        return incident_id

    def get_incident(self, incident_id: str) -> dict[str, Any] | None:
        with self._conn() as c:
            row = c.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone()
        return self._incident_dict(row)

    @staticmethod
    def _incident_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        d = dict(row)
        d["evidence"] = json.loads(d["evidence"]) if d.get("evidence") else []
        return d

    def set_status(self, incident_id: str, status: str) -> None:
        with self._conn() as c:
            c.execute("UPDATE incidents SET status=? WHERE id=?", (status, incident_id))

    def record_diagnosis(
        self, incident_id: str, root_cause: str, confidence: float, evidence: list[str], recommended_action: str
    ) -> None:
        if root_cause not in ROOT_CAUSES:
            raise ValueError(f"root_cause must be one of {ROOT_CAUSES}")
        with self._conn() as c:
            c.execute(
                "UPDATE incidents SET root_cause=?, confidence=?, evidence=?, recommended_action=?, diagnosed_at=? "
                "WHERE id=?",
                (root_cause, confidence, json.dumps(evidence), recommended_action, time.time(), incident_id),
            )

    # ---- proposals ---------------------------------------------------------
    def create_proposal(self, incident_id: str, action: str, rationale: str) -> str:
        if action not in ACTIONS:
            raise ValueError(f"action must be one of {ACTIONS}")
        proposal_id = "PRP-" + uuid.uuid4().hex[:8]
        with self._conn() as c:
            c.execute(
                "INSERT INTO proposals(id, incident_id, action, rationale, status, created_at) VALUES (?,?,?,?,?,?)",
                (proposal_id, incident_id, action, rationale, "pending", time.time()),
            )
        self.set_status(incident_id, "awaiting_approval")
        return proposal_id

    def get_proposal(self, proposal_id: str) -> dict[str, Any] | None:
        with self._conn() as c:
            row = c.execute("SELECT * FROM proposals WHERE id=?", (proposal_id,)).fetchone()
        return dict(row) if row else None

    def list_proposals(self, status: str | None = None, incident_id: str | None = None) -> list[dict[str, Any]]:
        q, args = "SELECT * FROM proposals WHERE 1=1", []
        if status:
            q += " AND status=?"
            args.append(status)
        if incident_id:
            q += " AND incident_id=?"
            args.append(incident_id)
        with self._conn() as c:
            return [dict(r) for r in c.execute(q + " ORDER BY created_at", args).fetchall()]

    def decide(self, proposal_id: str, approve: bool, decided_by: str) -> dict[str, Any]:
        """Human decision. Only a pending proposal can be decided, and only once."""
        with self._conn() as c:
            cur = c.execute(
                "UPDATE proposals SET status=?, decided_at=?, decided_by=? WHERE id=? AND status='pending'",
                ("approved" if approve else "rejected", time.time(), decided_by, proposal_id),
            )
            if cur.rowcount != 1:
                raise ValueError(f"proposal {proposal_id} is not pending (missing or already decided)")
        proposal = self.get_proposal(proposal_id)
        assert proposal is not None
        return proposal

    def claim_for_execution(self, proposal_id: str) -> dict[str, Any]:
        """Atomically move approved -> executing. Raises ApprovalRequired for anything else."""
        with self._conn() as c:
            cur = c.execute(
                "UPDATE proposals SET status='executing' WHERE id=? AND status='approved'", (proposal_id,)
            )
            if cur.rowcount != 1:
                raise ApprovalRequired(f"proposal {proposal_id} has not been approved by a human")
        proposal = self.get_proposal(proposal_id)
        assert proposal is not None
        return proposal

    def finish_execution(self, proposal_id: str, ok: bool, result: str) -> None:
        with self._conn() as c:
            c.execute(
                "UPDATE proposals SET status=?, result=? WHERE id=?",
                ("executed" if ok else "failed", result, proposal_id),
            )
