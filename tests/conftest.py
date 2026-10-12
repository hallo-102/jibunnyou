"""Select the committed replay configuration only inside regression tests."""
import pytest

@pytest.fixture(autouse=True)
def offline_test_scope(monkeypatch):
    # Tests may exercise offline replay; production invocation has no implicit opt-in.
    monkeypatch.setenv('KEIBA_FORWARD_SCOPE', 'offline_replay')
