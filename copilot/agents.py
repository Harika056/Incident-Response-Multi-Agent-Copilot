"""Supervisor + specialist agents built with Google ADK.

Shape
-----
supervisor (ReAct loop, owns the incident)
  |-- logs_specialist      (AgentTool; MCP: probe_endpoint, search_logs)
  |-- kafka_specialist     (AgentTool; MCP: get_kafka_consumer_lag)
  |-- database_specialist  (AgentTool; MCP: get_active_queries, get_table_stats, run_readonly_sql)
  |-- critic               (AgentTool; self-reflection pass, no tools)
  |-- submit_diagnosis     (FunctionTool; refuses unless the critic has reviewed)
  |-- propose_remediation  (FunctionTool; writes a PENDING proposal - cannot execute anything)
  `-- TaskFlow MCP         (create_ticket, add_comment, list_tickets - no delete tool exists)

Specialists are wrapped as AgentTools (not transferred to) so the supervisor keeps control
of the incident and sees each specialist's findings as an observation.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

from google.adk.agents import LlmAgent
from google.adk.tools import AgentTool, FunctionTool
from google.adk.tools.mcp_tool import McpToolset, StdioConnectionParams
from google.adk.tools.tool_context import ToolContext
from google.genai import types
from mcp import StdioServerParameters

from .config import Settings, get_settings
from .store import ACTIONS, ROOT_CAUSES, Store

REPO_ROOT = str(Path(__file__).resolve().parents[1])


# ----------------------------------------------------------------------------- tools
def _store() -> Store:
    return Store(get_settings().checkpoint_db)


def submit_diagnosis(
    root_cause: str,
    confidence: float,
    evidence: list[str],
    recommended_action: str,
    tool_context: ToolContext,
) -> dict[str, Any]:
    """Record the final diagnosis for this incident.

    Args:
        root_cause: One of SERVICE_OUTAGE, KAFKA_CONSUMER_LAG, SLOW_QUERY, OTHER.
        confidence: Number between 0 and 1.
        evidence: Concrete observations (with values) that support the root cause.
        recommended_action: One of restart_service, resume_consumer, cancel_slow_queries, or none.
    """
    if not tool_context.state.get("reflected"):
        return {"error": "Run the critic (self-reflection) on your hypothesis before submitting a diagnosis."}
    if root_cause not in ROOT_CAUSES:
        return {"error": f"root_cause must be one of {list(ROOT_CAUSES)}"}
    if not 0.0 <= confidence <= 1.0:
        return {"error": "confidence must be between 0 and 1"}
    if not evidence:
        return {"error": "evidence must not be empty"}
    incident_id = tool_context.state["incident_id"]
    _store().record_diagnosis(incident_id, root_cause, confidence, evidence, recommended_action)
    tool_context.state["reflected"] = False  # a revised diagnosis needs a fresh critic pass
    return {"status": "recorded", "incident_id": incident_id}


def propose_remediation(action: str, rationale: str, tool_context: ToolContext) -> dict[str, Any]:
    """Propose a remediation for HUMAN approval. This does NOT execute anything.

    Args:
        action: One of restart_service, resume_consumer, cancel_slow_queries.
        rationale: Why this action addresses the diagnosed root cause, and its risk.
    """
    if action not in ACTIONS:
        return {"error": f"action must be one of {list(ACTIONS)}"}
    store = _store()
    incident_id = tool_context.state["incident_id"]
    incident = store.get_incident(incident_id)
    if not incident or not incident.get("root_cause"):
        return {"error": "Call submit_diagnosis before proposing a remediation."}
    proposal_id = store.create_proposal(incident_id, action, rationale)
    return {
        "status": "PENDING_HUMAN_APPROVAL",
        "proposal_id": proposal_id,
        "note": "Nothing has been executed. Stop now and tell the human what is awaiting approval.",
    }


def _mark_reflection(tool, args, tool_context, tool_response):  # after_tool_callback
    if getattr(tool, "name", "") == "critic":
        tool_context.state["reflected"] = True
    return None


# --------------------------------------------------------------------------- MCP wiring
def _stdio_toolset(module: str, tool_filter: list[str], settings: Settings) -> McpToolset:
    env = dict(os.environ)
    env["PYTHONPATH"] = REPO_ROOT + os.pathsep + env.get("PYTHONPATH", "")
    env["TASKFLOW_JWT"] = settings.taskflow_jwt
    return McpToolset(
        connection_params=StdioConnectionParams(
            server_params=StdioServerParameters(command=sys.executable, args=["-m", module], env=env),
            timeout=30.0,
        ),
        tool_filter=tool_filter,
    )


def _gen_config() -> types.GenerateContentConfig:
    return types.GenerateContentConfig(temperature=0.0)


# ------------------------------------------------------------------------------ agents
def build_specialists(settings: Settings, with_mcp: bool = True, model: Any = None):
    model = model or settings.model
    diag = "copilot.mcp_servers.orderhub_diag"

    def tools(names: list[str]) -> list:
        return [_stdio_toolset(diag, names, settings)] if with_mcp else []

    common = (
        "You are a specialist on an incident-response team. You only INVESTIGATE; you never "
        "change anything. Use your tools, quote concrete values (counts, ms, offsets, timestamps), "
        "and state plainly when evidence is absent or inconclusive. Do not guess beyond the data. "
        "Keep your report under 150 words."
    )
    logs = LlmAgent(
        name="logs_specialist",
        model=model,
        description="Checks OrderHub health, endpoint latency and application logs (errors/warnings).",
        instruction=common
        + " Your focus: service health/reachability (probe_endpoint on /actuator/health and "
        "/orders/recent) and application logs (search_logs: start with level ERROR, then WARN). "
        "Report the error/warn counts, the dominant messages and any timing signals.",
        tools=tools(["probe_endpoint", "search_logs"]),
        generate_content_config=_gen_config(),
    )
    kafka = LlmAgent(
        name="kafka_specialist",
        model=model,
        description="Inspects Kafka consumer-group lag and state for OrderHub's order consumer.",
        instruction=common
        + " Your focus: get_kafka_consumer_lag. Report total lag, per-partition committed vs log-end "
        "offsets, and the consumer group state/member count. Say whether lag looks like a stalled "
        "consumer (committed offsets not advancing while log-end grows) or a healthy one.",
        tools=tools(["get_kafka_consumer_lag"]),
        generate_content_config=_gen_config(),
    )
    db = LlmAgent(
        name="database_specialist",
        model=model,
        description="Inspects Postgres: long-running queries, table scan stats, ad-hoc read-only SELECTs.",
        instruction=common
        + " Your focus: get_active_queries (try min_duration_ms=500), get_table_stats, and, if needed, "
        "run_readonly_sql (SELECT only). Report any long-running statements with their duration and text.",
        tools=tools(["get_active_queries", "get_table_stats", "run_readonly_sql"]),
        generate_content_config=_gen_config(),
    )
    critic = LlmAgent(
        name="critic",
        model=model,
        description=(
            "Self-reflection pass. Send it your current hypothesis and the evidence gathered; it "
            "returns contradicting evidence, alternative explanations and missing checks."
        ),
        instruction=(
            "You are a skeptical senior SRE reviewing a colleague's incident hypothesis. You will "
            "receive a hypothesis and the evidence behind it. Respond with: (1) VERDICT: SUPPORTED or "
            "NEEDS_MORE_EVIDENCE; (2) any evidence that contradicts the hypothesis; (3) the strongest "
            "alternative explanations among SERVICE_OUTAGE, KAFKA_CONSUMER_LAG, SLOW_QUERY; (4) specific "
            "checks that would rule them in or out. Be concise (under 120 words). You have no tools."
        ),
        generate_content_config=_gen_config(),
    )
    return logs, kafka, db, critic


SUPERVISOR_INSTRUCTION = """You are the incident commander for OrderHub, an order-processing service
(Spring Boot -> Kafka topic orders.created -> consumer -> Postgres). Incident id: {incident_id}

Work in a ReAct loop: state a short thought, act by delegating to a specialist, read the observation,
repeat. Your specialists: logs_specialist (health, latency, logs), kafka_specialist (consumer lag),
database_specialist (Postgres queries). You cannot read logs, Kafka or the database yourself.

Candidate root causes (pick exactly one):
- SERVICE_OUTAGE: OrderHub itself is unhealthy/unreachable or failing requests.
- KAFKA_CONSUMER_LAG: the service is up but the order consumer has stopped keeping up, so lag grows.
- SLOW_QUERY: the service is up but long-running Postgres queries are degrading it.
- OTHER: none of the above fit the evidence.

Rules:
1. Consult at least two specialists and actively try to RULE OUT the alternatives, not just confirm
   your first guess. Symptoms overlap, so look for the discriminating signal.
2. Before submitting, send your hypothesis and evidence to the critic (self-reflection). If it says
   NEEDS_MORE_EVIDENCE, gather that evidence first. Revise the hypothesis if the evidence contradicts it.
3. Call submit_diagnosis with root_cause, confidence (0-1), concrete evidence and recommended_action.
4. Then call propose_remediation with the matching action (restart_service for SERVICE_OUTAGE,
   resume_consumer for KAFKA_CONSUMER_LAG, cancel_slow_queries for SLOW_QUERY). You can NOT execute
   remediation; it waits for a human. After proposing, STOP and give a brief summary for the human:
   root cause, key evidence, and the proposal awaiting approval. Do not ask questions in chat.
5. If you are later told the human APPROVED and the action was executed, verify recovery using the
   specialists, then file a ticket with create_ticket (SEV2 if customer-facing orders were failing,
   otherwise SEV3) summarising cause, evidence, action and verified outcome. If told it was REJECTED,
   file a ticket describing the diagnosis and that manual remediation is required.
Never claim an action was taken unless you were told it was executed.
"""


def build_supervisor(
    settings: Settings | None = None,
    *,
    with_mcp: bool = True,
    model: Any = None,
    specialist_model: Any = None,
) -> LlmAgent:
    settings = settings or get_settings()
    model = model or settings.model
    logs, kafka, db, critic = build_specialists(settings, with_mcp=with_mcp, model=specialist_model or model)

    tools: list = [
        AgentTool(agent=logs),
        AgentTool(agent=kafka),
        AgentTool(agent=db),
        AgentTool(agent=critic),
        FunctionTool(submit_diagnosis),
        FunctionTool(propose_remediation),
    ]
    if with_mcp:
        tools.append(
            _stdio_toolset("copilot.mcp_servers.taskflow", ["create_ticket", "add_comment", "list_tickets"], settings)
        )

    return LlmAgent(
        name="incident_supervisor",
        model=model,
        description="Incident commander that delegates to specialists and gates remediation on human approval.",
        instruction=SUPERVISOR_INSTRUCTION,
        tools=tools,
        after_tool_callback=_mark_reflection,
        generate_content_config=_gen_config(),
    )
