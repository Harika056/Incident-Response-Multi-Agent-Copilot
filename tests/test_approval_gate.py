import httpx
import pytest

from copilot.config import Settings
from copilot.remediation import execute_proposal
from copilot.store import ApprovalRequired, Store


@pytest.fixture
def store(tmp_path):
    return Store(str(tmp_path / "t.db"))


@pytest.fixture
def incident(store):
    return store.create_incident("alert", "sess-1")


def ok_client(calls):
    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, request.url.path, request.headers.get("X-Admin-Token")))
        return httpx.Response(200, json={"fixed": True})
    return httpx.Client(transport=httpx.MockTransport(handler))


def test_pending_proposal_cannot_execute(store, incident):
    pid = store.create_proposal(incident, "restart_service", "why")
    calls = []
    with pytest.raises(ApprovalRequired):
        execute_proposal(store, pid, Settings(), ok_client(calls))
    assert calls == []                      # nothing reached OrderHub
    assert store.get_proposal(pid)["status"] == "pending"


def test_rejected_proposal_cannot_execute(store, incident):
    pid = store.create_proposal(incident, "restart_service", "why")
    store.decide(pid, approve=False, decided_by="alice")
    calls = []
    with pytest.raises(ApprovalRequired):
        execute_proposal(store, pid, Settings(), ok_client(calls))
    assert calls == []


def test_approved_proposal_executes_exactly_once(store, incident):
    pid = store.create_proposal(incident, "resume_consumer", "why")
    store.decide(pid, approve=True, decided_by="alice")
    calls = []
    out = execute_proposal(store, pid, Settings(orderhub_admin_token="tok"), ok_client(calls))
    assert out["ok"] is True
    assert calls == [("POST", "/admin/remediate/resume_consumer", "tok")]
    assert store.get_proposal(pid)["status"] == "executed"
    with pytest.raises(ApprovalRequired):   # replay is refused
        execute_proposal(store, pid, Settings(), ok_client(calls))
    assert len(calls) == 1


def test_decision_is_single_shot(store, incident):
    pid = store.create_proposal(incident, "restart_service", "why")
    store.decide(pid, approve=False, decided_by="alice")
    with pytest.raises(ValueError):
        store.decide(pid, approve=True, decided_by="mallory")   # cannot flip a rejection


def test_failed_execution_is_recorded(store, incident):
    pid = store.create_proposal(incident, "restart_service", "why")
    store.decide(pid, approve=True, decided_by="alice")
    client = httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(500, text="boom")))
    out = execute_proposal(store, pid, Settings(), client)
    assert out["ok"] is False
    assert store.get_proposal(pid)["status"] == "failed"


def test_unknown_action_and_root_cause_rejected(store, incident):
    with pytest.raises(ValueError):
        store.create_proposal(incident, "drop_database", "why")
    with pytest.raises(ValueError):
        store.record_diagnosis(incident, "ALIENS", 0.9, ["x"], "none")


def test_state_survives_reopen(tmp_path):
    path = str(tmp_path / "persist.db")
    s1 = Store(path)
    inc = s1.create_incident("alert", "sess-9")
    pid = s1.create_proposal(inc, "restart_service", "why")
    s2 = Store(path)                         # "another process"
    assert s2.get_proposal(pid)["status"] == "pending"
    assert s2.get_incident(inc)["session_id"] == "sess-9"
    assert s2.get_incident(inc)["status"] == "awaiting_approval"
