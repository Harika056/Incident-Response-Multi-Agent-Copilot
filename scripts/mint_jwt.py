"""Mint a dev JWT for the copilot's TaskFlow MCP server.

    export TASKFLOW_JWT=$(python scripts/mint_jwt.py)

The token carries ONLY read/create/comment roles. There is intentionally no flag to add
TICKET_DELETE; mint an admin token by hand if you ever need one.
"""
import os
import sys
import time

import jwt

secret = os.environ.get("TASKFLOW_JWT_SECRET", "dev-only-secret-change-me-32-bytes-min!!")
if len(secret.encode()) < 32:
    sys.exit("TASKFLOW_JWT_SECRET must be at least 32 bytes for HS256")

claims = {
    "sub": "incident-copilot",
    "roles": ["TICKET_READ", "TICKET_CREATE", "TICKET_COMMENT"],
    "iat": int(time.time()),
    "exp": int(time.time()) + int(os.environ.get("TASKFLOW_JWT_TTL_SECONDS", "3600")),
}
print(jwt.encode(claims, secret, algorithm="HS256"))
