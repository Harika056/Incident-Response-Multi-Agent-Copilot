"""Run / pause / approve / resume the incident-response copilot.

    python -m copilot.run investigate "OrderHub order processing is degraded"
    python -m copilot.run pending
    python -m copilot.run approve PRP-xxxxxxxx      # human decision -> execute -> resume agent
    python -m copilot.run reject  PRP-xxxxxxxx --reason "wait for DBA"
    python -m copilot.run show INC-xxxxxxxx

Pause/resume: the agent run ends after ``propose_remediation``. The ADK session
(DatabaseSessionService) and our incident/proposal tables persist on disk, so ``approve`` can
run in a different process hours later and continue the SAME session with full context.
"""
from __future__ import annotations

import argparse
import asyncio
import getpass
import json
import sys
import uuid
from typing import Any, Callable

from google.adk.agents.run_config import RunConfig
from google.adk.runners import Runner
from google.adk.sessions import BaseSessionService, DatabaseSessionService
from google.genai import types

from .agents import build_supervisor
from .config import Settings, get_settings
from .remediation import execute_proposal
from .store import Store

APP_NAME = "incident-copilot"
USER_ID = "oncall"


def make_session_service(settings: Settings) -> BaseSessionService:
    import os

    os.makedirs(settings.state_dir, exist_ok=True)
    return DatabaseSessionService(settings.adk_session_url)


async def _drive(
    runner: Runner,
    session_id: str,
    message: str,
    settings: Settings,
    on_event: Callable[[str, str], None] | None,
) -> str:
    final_text = ""
    content = types.Content(role="user", parts=[types.Part(text=message)])
    async for event in runner.run_async(
        user_id=USER_ID,
        session_id=session_id,
        new_message=content,
        run_config=RunConfig(max_llm_calls=settings.max_llm_calls),
    ):
        if on_event:
            for fc in event.get_function_calls() or []:
                on_event(event.author, f"-> {fc.name}({json.dumps(dict(fc.args or {}), default=str)[:160]})")
        if event.is_final_response() and event.content and event.content.parts:
            text = "".join(p.text or "" for p in event.content.parts)
            if text.strip() and event.author == "incident_supervisor":
                final_text = text
    return final_text


async def investigate(
    alert: str,
    *,
    settings: Settings | None = None,
    supervisor=None,
    session_service: BaseSessionService | None = None,
    store: Store | None = None,
    on_event: Callable[[str, str], None] | None = None,
) -> dict[str, Any]:
    settings = settings or get_settings()
    store = store or Store(settings.checkpoint_db)
    session_service = session_service or make_session_service(settings)
    supervisor = supervisor or build_supervisor(settings)

    session_id = uuid.uuid4().hex
    incident_id = store.create_incident(alert, session_id)
    await session_service.create_session(
        app_name=APP_NAME,
        user_id=USER_ID,
        session_id=session_id,
        state={"incident_id": incident_id, "reflected": False},
    )

    runner = Runner(app_name=APP_NAME, agent=supervisor, session_service=session_service)
    final = await _drive(runner, session_id, f"ALERT: {alert}\nIncident id: {incident_id}", settings, on_event)
    return {"incident_id": incident_id, "summary": final, "incident": store.get_incident(incident_id)}


async def resume(
    incident_id: str,
    message: str,
    *,
    settings: Settings | None = None,
    supervisor=None,
    session_service: BaseSessionService | None = None,
    store: Store | None = None,
    on_event: Callable[[str, str], None] | None = None,
) -> str:
    settings = settings or get_settings()
    store = store or Store(settings.checkpoint_db)
    incident = store.get_incident(incident_id)
    if incident is None:
        raise SystemExit(f"unknown incident {incident_id}")
    session_service = session_service or make_session_service(settings)
    supervisor = supervisor or build_supervisor(settings)
    runner = Runner(app_name=APP_NAME, agent=supervisor, session_service=session_service)
    return await _drive(runner, incident["session_id"], message, settings, on_event)


# ------------------------------------------------------------------------------- CLI
def _print_event(author: str, text: str) -> None:
    print(f"  [{author}] {text}", file=sys.stderr)


def _show_pending(store: Store) -> None:
    pending = store.list_proposals(status="pending")
    if not pending:
        print("No proposals awaiting approval.")
        return
    for p in pending:
        print(f"{p['id']}  incident={p['incident_id']}  action={p['action']}\n    why: {p['rationale']}")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="copilot")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("investigate", help="start an investigation from an alert")
    p.add_argument("alert")
    sub.add_parser("pending", help="list proposals awaiting human approval")
    p = sub.add_parser("show", help="show an incident and its proposals")
    p.add_argument("incident_id")
    p = sub.add_parser("approve", help="approve a proposal, execute it, resume the agent")
    p.add_argument("proposal_id")
    p.add_argument("--yes", action="store_true", help="skip the confirmation prompt")
    p.add_argument("--no-resume", action="store_true", help="execute but do not resume the agent")
    p = sub.add_parser("reject", help="reject a proposal and resume the agent")
    p.add_argument("proposal_id")
    p.add_argument("--reason", default="no reason given")
    p.add_argument("--no-resume", action="store_true")
    args = parser.parse_args(argv)

    settings = get_settings()
    store = Store(settings.checkpoint_db)

    if args.cmd == "investigate":
        result = asyncio.run(investigate(args.alert, settings=settings, store=store, on_event=_print_event))
        print(f"\nIncident {result['incident_id']}\n{result['summary']}\n")
        _show_pending(store)
        return

    if args.cmd == "pending":
        _show_pending(store)
        return

    if args.cmd == "show":
        print(json.dumps({"incident": store.get_incident(args.incident_id),
                          "proposals": store.list_proposals(incident_id=args.incident_id)}, indent=2, default=str))
        return

    proposal = store.get_proposal(args.proposal_id)
    if proposal is None:
        raise SystemExit(f"unknown proposal {args.proposal_id}")
    human = getpass.getuser()

    if args.cmd == "approve":
        print(f"Proposal {proposal['id']}: {proposal['action']}\n  why: {proposal['rationale']}")
        if not args.yes and input("Approve and execute? [y/N] ").strip().lower() != "y":
            raise SystemExit("not approved; nothing executed")
        store.decide(proposal["id"], approve=True, decided_by=human)
        outcome = execute_proposal(store, proposal["id"], settings)
        print(f"executed={outcome['ok']} result={outcome['result']}")
        message = (
            f"HUMAN DECISION: {human} APPROVED proposal {proposal['id']} ({proposal['action']}). "
            f"It was executed. Executor result ok={outcome['ok']}: {outcome['result']}. "
            "Verify recovery with your specialists, then file the ticket."
        )
    else:  # reject
        store.decide(proposal["id"], approve=False, decided_by=human)
        message = (
            f"HUMAN DECISION: {human} REJECTED proposal {proposal['id']} ({proposal['action']}). "
            f"Reason: {args.reason}. Nothing was executed. File a ticket noting manual follow-up is needed."
        )

    if args.no_resume:
        return
    summary = asyncio.run(
        resume(proposal["incident_id"], message, settings=settings, store=store, on_event=_print_event)
    )
    store.set_status(proposal["incident_id"], "resolved" if args.cmd == "approve" else "closed")
    print(f"\n{summary}")


if __name__ == "__main__":
    main()
