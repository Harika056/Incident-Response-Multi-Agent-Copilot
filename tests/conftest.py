import pytest


@pytest.fixture(autouse=True)
def isolated_state(tmp_path, monkeypatch):
    """Every test gets its own checkpoint dir and no ambient secrets."""
    monkeypatch.setenv("COPILOT_STATE_DIR", str(tmp_path / "state"))
    monkeypatch.delenv("TASKFLOW_JWT", raising=False)
    return tmp_path
