import httpx
import jwt
import pytest

from copilot.config import Settings
from copilot.mcp_servers import taskflow

SECRET = "x" * 40


def token(roles):
    return jwt.encode({"sub": "incident-copilot", "roles": roles}, SECRET, algorithm="HS256")


GOOD = ["TICKET_READ", "TICKET_CREATE", "TICKET_COMMENT"]


async def test_no_destructive_tools_exposed():
    names = {t.name for t in await taskflow.mcp.list_tools()}
    assert names == {"create_ticket", "get_ticket", "list_tickets", "add_comment"}
    assert not any(w in n for n in names for w in ("delete", "remove", "update", "close", "admin"))


def test_least_privilege_token_accepted():
    taskflow.assert_least_privilege(token(GOOD))


@pytest.mark.parametrize("roles,msg", [
    (GOOD + ["TICKET_DELETE"], "over-privileged"),
    (GOOD + ["TICKET_ADMIN"], "over-privileged"),
    (["TICKET_READ"], "missing"),
])
def test_bad_tokens_refused(roles, msg):
    with pytest.raises(PermissionError, match=msg):
        taskflow.assert_least_privilege(token(roles))


def test_missing_token_refused():
    with pytest.raises(PermissionError):
        taskflow.assert_least_privilege("")


def test_client_sends_bearer_and_maps_errors():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["auth"] = request.headers["Authorization"]
        seen["body"] = request.content.decode()
        if request.url.path == "/api/tickets/TF-9":
            return httpx.Response(403, text="forbidden")
        return httpx.Response(201, json={"id": "TF-1"})

    tok = token(GOOD)
    tf = taskflow.TaskflowClient(Settings(taskflow_jwt=tok, taskflow_url="http://tf"),
                                 httpx.Client(transport=httpx.MockTransport(handler)))
    assert tf.create_ticket("t", "d", "SEV2", "INC-1") == {"id": "TF-1"}
    assert seen["auth"] == f"Bearer {tok}"
    assert "INC-1" in seen["body"]
    assert tf.get_ticket("TF-9")["error"].startswith("TaskFlow returned HTTP 403")
    assert "severity" in tf.create_ticket("t", "d", "URGENT")["error"]


def test_overprivileged_token_never_hits_network():
    def handler(request):
        raise AssertionError("must not call TaskFlow with an over-privileged token")

    tf = taskflow.TaskflowClient(Settings(taskflow_jwt=token(GOOD + ["TICKET_DELETE"])),
                                 httpx.Client(transport=httpx.MockTransport(handler)))
    assert "over-privileged" in tf.create_ticket("t", "d", "SEV3")["error"]
