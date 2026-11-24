"""Environment-driven configuration shared by the agents, MCP servers and eval harness."""
from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


@dataclass(frozen=True)
class Settings:
    # --- LLM -----------------------------------------------------------------
    model: str = field(default_factory=lambda: _env("COPILOT_MODEL", "gemini-2.5-flash"))
    max_llm_calls: int = field(default_factory=lambda: int(_env("COPILOT_MAX_LLM_CALLS", "60")))

    # --- OrderHub ------------------------------------------------------------
    orderhub_url: str = field(default_factory=lambda: _env("ORDERHUB_URL", "http://localhost:8080"))
    orderhub_log_path: str = field(default_factory=lambda: _env("ORDERHUB_LOG_PATH", "./logs/orderhub.log"))
    # Admin token is NOT given to the diagnostic MCP server. Only the human-approved
    # remediation executor and the eval fault injector use it.
    orderhub_admin_token: str = field(default_factory=lambda: _env("ORDERHUB_ADMIN_TOKEN", "dev-admin-token"))

    # --- Kafka ---------------------------------------------------------------
    kafka_bootstrap: str = field(default_factory=lambda: _env("KAFKA_BOOTSTRAP", "localhost:29092"))
    kafka_group: str = field(default_factory=lambda: _env("KAFKA_GROUP", "orderhub-inventory"))
    kafka_topic: str = field(default_factory=lambda: _env("KAFKA_TOPIC", "orders.created"))

    # --- Postgres (read-only diagnostic role) --------------------------------
    pg_dsn_ro: str = field(
        default_factory=lambda: _env("PG_DSN_RO", "postgresql://diag_ro:diag_ro@localhost:5432/orderhub")
    )
    pg_statement_timeout_ms: int = field(default_factory=lambda: int(_env("PG_STATEMENT_TIMEOUT_MS", "3000")))

    # --- TaskFlow ------------------------------------------------------------
    taskflow_url: str = field(default_factory=lambda: _env("TASKFLOW_URL", "http://localhost:8081"))
    taskflow_jwt: str = field(default_factory=lambda: _env("TASKFLOW_JWT", ""))

    # --- Checkpointing -------------------------------------------------------
    state_dir: str = field(default_factory=lambda: _env("COPILOT_STATE_DIR", "./state"))

    @property
    def checkpoint_db(self) -> str:
        return os.path.join(self.state_dir, "copilot.db")

    @property
    def adk_session_url(self) -> str:
        return "sqlite+aiosqlite:///" + os.path.abspath(os.path.join(self.state_dir, "adk_sessions.db"))


def get_settings() -> Settings:
    return Settings()
