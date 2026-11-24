"""End-to-end: real ADK supervisor + real tools + real SQLite session/checkpoint stores,
driven by a scripted fake LLM (no network, no API key).

Proves: reflection is enforced, remediation is only *proposed*, the run pauses, and a fresh
service instance (simulating a new process) resumes the SAME session after a human approves.
"""
import httpx
from google.adk.models.base_llm import BaseLlm
from google.adk.models.llm_response import LlmResponse
from google.genai import types

from copilot.agents import build_supervisor
from copilot.config import get_settings
from copilot.remediation import execute_proposal
from copilot.run import APP_NAME, USER_ID, investigate, make_session_service, resume
from copilot.store import Store


class ScriptedLlm(BaseLlm):
    model: str = "scripted"
    script: list = []
    cursor: int = 0
    seen: list = []

    @classmethod
    def supported_models(cls):
        return ["scripted"]

    async def generate_content_async(self, llm_request, stream=False):
        self.seen.append(str(llm_request.contents))
        part = self.script[min(self.cursor, len(self.script) - 1)]
        self.cursor += 1
        yield LlmResponse(content=types.Content(role="model", parts=[part]))


def fc(name, **args):
    return types.Part(function_call=types.FunctionCall(name=name, args=args))


def text(t):
    return types.Part(text=t)


def function_responses(session):
    out = []
    for ev in session.events:
        for fr in ev.get_function_responses() or []:
            out.append((fr.name, fr.response))
    return out


async def test_pause_approve_resume_across_processes():
    settings = get_settings()
    store = Store(settings.checkpoint_db)

    # ---------------- phase 1: investigate, then pause at propose_remediation ----------------
    diagnosis = dict(root_cause="KAFKA_CONSUMER_LAG", confidence=0.9,
                     evidence=["total lag 500, committed offsets static"], recommended_action="resume_consumer")
    supervisor_llm = ScriptedLlm(seen=[], script=[
        fc("submit_diagnosis", **diagnosis),                       # premature: must be refused
        fc("kafka_specialist", request="check consumer lag"),
        fc("critic", request="hypothesis: stalled consumer; evidence: lag 500"),
        fc("submit_diagnosis", **diagnosis),                       # now allowed
        fc("propose_remediation", action="resume_consumer", rationale="consumer is paused; resume it"),
        text("Root cause KAFKA_CONSUMER_LAG. Proposal awaiting approval."),
    ])
    helper_llm = ScriptedLlm(seen=[], script=[text("Lag is 500 and not advancing. VERDICT: SUPPORTED")])
    supervisor = build_supervisor(settings, with_mcp=False, model=supervisor_llm, specialist_model=helper_llm)

    svc1 = make_session_service(settings)
    result = await investigate("alert", settings=settings, supervisor=supervisor, session_service=svc1, store=store)
    incident_id = result["incident_id"]

    session = await svc1.get_session(app_name=APP_NAME, user_id=USER_ID, session_id=result["incident"]["session_id"])
    responses = function_responses(session)
    first_submit = next(r for n, r in responses if n == "submit_diagnosis")
    assert "critic" in first_submit["error"]                      # reflection gate held
    assert any(n == "critic" for n, _ in responses)

    incident = store.get_incident(incident_id)
    assert incident["root_cause"] == "KAFKA_CONSUMER_LAG" and incident["status"] == "awaiting_approval"
    (proposal,) = store.list_proposals(incident_id=incident_id)
    assert proposal["status"] == "pending"                        # proposed, NOT executed
    assert proposal["action"] == "resume_consumer"
    events_before = len(session.events)

    # ---------------- human decision (separate step) ----------------
    store.decide(proposal["id"], approve=True, decided_by="alice")
    calls = []
    client = httpx.Client(transport=httpx.MockTransport(
        lambda r: (calls.append(r.url.path), httpx.Response(200, json={"fixed": True}))[1]))
    assert execute_proposal(store, proposal["id"], settings, client)["ok"] is True
    assert calls == ["/admin/remediate/resume_consumer"]

    # ---------------- phase 2: "new process" resumes the same session ----------------
    resume_llm = ScriptedLlm(seen=[], script=[text("Recovery verified. Ticket filed.")])
    supervisor2 = build_supervisor(settings, with_mcp=False, model=resume_llm, specialist_model=helper_llm)
    svc2 = make_session_service(settings)                         # fresh service, same SQLite file
    final = await resume(incident_id, "HUMAN DECISION: alice APPROVED proposal. It was executed.",
                         settings=settings, supervisor=supervisor2, session_service=svc2, store=store)

    assert final == "Recovery verified. Ticket filed."
    session2 = await svc2.get_session(app_name=APP_NAME, user_id=USER_ID, session_id=result["incident"]["session_id"])
    assert len(session2.events) > events_before                   # appended to the same session
    # The resumed model saw phase-1 history (the pending proposal) plus the human's decision.
    assert "PENDING_HUMAN_APPROVAL" in resume_llm.seen[0]
    assert "APPROVED" in resume_llm.seen[0]


async def test_propose_without_diagnosis_is_refused():
    from types import SimpleNamespace
    from copilot.agents import propose_remediation
    store = Store(get_settings().checkpoint_db)
    inc = store.create_incident("a", "s")
    ctx = SimpleNamespace(state={"incident_id": inc})
    assert "submit_diagnosis" in propose_remediation("restart_service", "why", ctx)["error"]
    assert store.list_proposals(incident_id=inc) == []
