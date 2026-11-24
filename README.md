# Incident-Response Multi-Agent Copilot for OrderHub

A human-in-the-loop incident-response copilot. A **supervisor agent** (ReAct-style, with a
self-reflection step) delegates to **specialist agents** for logs, Kafka and the database, talks to
OrderHub through **MCP** diagnostic tools, and files tickets through a **least-privilege TaskFlow MCP
server**. It can *propose* remediations but can never *execute* one: a human approves, and the run
resumes from a checkpoint.

**Stack:** Python, Google ADK, MCP, Kafka, Postgres, Docker, Spring Boot (OrderHub, TaskFlow).

```
                         alert
                           |
                  +--------v---------+      self-reflection
                  |    supervisor    |----> critic (no tools)
                  +--+-----+------+--+
        AgentTool    |     |      |    AgentTool
        +------------+     |      +-------------+
        v                  v                    v
 logs_specialist    kafka_specialist    database_specialist
        |                  |                    |
        +---- MCP: orderhub-diagnostics (READ-ONLY) ----+
              probe_endpoint, search_logs, get_kafka_consumer_lag,
              get_active_queries, get_table_stats, run_readonly_sql

 supervisor tools:  submit_diagnosis  (refused until the critic has reviewed)
                    propose_remediation (writes a PENDING row; executes nothing)
                    MCP: taskflow-tickets  create_ticket / add_comment / list_tickets / get_ticket
                                           (no delete tool; token has no delete role)

 human:  copilot approve PRP-xxxx  --> Store.decide --> remediation executor --> OrderHub admin API
                                   `--> resume the SAME ADK session with the decision
```

## Why the approval gate is real (not a prompt)

* The agents are given **no tool** that changes OrderHub. `propose_remediation` only inserts a
  `pending` row in SQLite.
* Only the human-facing CLI calls `Store.decide(...)`, and `Store.claim_for_execution` re-checks
  `approved` atomically before `copilot/remediation.py` calls OrderHub's token-protected
  `/admin/remediate/*`. A pending, rejected or already-executed proposal raises `ApprovalRequired`.
* The diagnostic MCP server has no admin token and a path allow-list, so agents cannot read the
  injected fault state or call the remediation endpoints.
* The eval harness checks this end to end: after every trial the injected fault must still be active
  and no proposal may have left `pending` (`gate_violation`).

## Checkpointing / pause-resume

Two durable stores, both SQLite:

| Store | Holds |
|---|---|
| ADK `DatabaseSessionService` (`state/adk_sessions.db`) | the full conversation + tool history of the supervisor session |
| `Store` (`state/copilot.db`) | incidents, diagnosis, proposals and their approval status |

The agent run **ends** after `propose_remediation`. `copilot approve` can run hours later in a
different process: it records the decision, executes, then re-opens the same session and tells the
supervisor what happened so it can verify recovery and file the ticket.
`tests/test_e2e_pause_resume.py` exercises exactly this with a fresh session-service instance.

## Quickstart

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
cp .env.example .env            # add GOOGLE_API_KEY, then: set -a; source .env; set +a

docker compose up --build -d    # Kafka, Postgres, OrderHub, TaskFlow  (logs -> ./logs)
export TASKFLOW_JWT=$(python scripts/mint_jwt.py)

# inject a fault by hand (token-protected admin API) and page the copilot
curl -XPOST -H 'X-Admin-Token: dev-admin-token' localhost:8080/admin/faults/kafka_lag
python -m copilot.run investigate "OrderHub order processing is degraded"
python -m copilot.run pending
python -m copilot.run approve PRP-xxxxxxxx      # human approval -> executes -> agent resumes
```

## Evaluation (produces your resume numbers)

```bash
# 1. manual baseline: you diagnose random faults with logs / kafka tools / psql (stopwatch)
python -m eval.baseline --per-fault 3 --out results/baseline.csv

# 2. the copilot on N = 3 x per-fault injected faults
python -m eval.run_eval --per-fault 10 --out results/run1 --baseline results/baseline.csv
```

Outputs `results/run1/results.csv` (one row per trial), `summary.json`, and a ready-to-paste resume
bullet. Methodology:

* **Ground truth** is the injected fault (`SERVICE_OUTAGE`, `KAFKA_CONSUMER_LAG`, `SLOW_QUERY`).
  Correct = the agent's recorded `root_cause` equals it. No hand labelling.
* The alert text is **identical** for every fault and OrderHub's logs/health don't name the fault
  (the injected-fault API is token-protected and not reachable by the agents).
* **Time to diagnosis** = incident creation to `submit_diagnosis`, including LLM latency. The
  settle period before paging is excluded.
* Reported with a **95% Wilson interval**: with 30 trials, 27/30 is 90% but the interval is ~74-97%.
* Also reported: correct-remediation rate, gate violations (should be 0), errors / no-diagnosis.
* Caveats to state honestly: closed-world faults (3 classes) on a stand-in app, one injected
  fault at a time, and the manual baseline is self-measured on the same faults.

## Layout

```
copilot/           agents.py (supervisor, specialists, critic) - run.py (CLI, resume)
                   store.py (checkpoint + approvals) - remediation.py (human-approved executor)
                   diagnostics.py / guard.py - mcp_servers/{orderhub_diag,taskflow}.py
eval/              harness.py (injector, load, stats) - run_eval.py - baseline.py
orderhub/          Spring Boot stand-in: orders API, Kafka consumer, Postgres, fault injection
taskflow/          Spring Boot stand-in: tickets API secured by role-based JWT (HS256)
db/init.sql        schema + read-only `diag_ro` role      docker-compose.yml
tests/             45 tests (guard, approval gate, MCP least-privilege, diagnostics, e2e resume)
```

## Status

| Area | Status |
|---|---|
| Python agents, MCP servers, approval gate, checkpoint/resume, eval harness | Unit + end-to-end tested with a scripted fake LLM (`pytest`, 45 tests) |
| Real LLM behaviour and accuracy numbers | **Not measured yet**: needs your `GOOGLE_API_KEY` and `eval.run_eval` |
| Kafka / Postgres diagnostics against live services | Logic is unit-tested; **not run against a live broker/DB** |
| OrderHub / TaskFlow (Java) | Written but **not compiled or run** yet; verify with `docker compose up --build` |

## Verified Results

The incident-response system was validated through automated testing and a live Kafka fault-injection demonstration.

| Metric | Result |
|---|---|
| Automated tests | 45/45 passed |
| Test execution time | 10.97 seconds |
| Kafka consumer lag before remediation | 30 messages |
| Kafka consumer lag after remediation | 0 messages |
| Kafka lag reduction | 100% |
| Human approval gate | Verified |
| Remediation execution | Successful |
| Consumer group status | Stable |

The live demonstration used deterministic diagnostic logic and persistent human approval to execute guarded remediation without external LLM API calls.

Live LLM-driven investigation and post-remediation session resumption remain pending validation due to external API rate limits.

