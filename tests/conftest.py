import re

import pytest

# Imported first: it locks down SpiriSynq's default session.
from synq_expose.testing import Network

from synq_expose.config import ExecConfig, ExposeConfig, SandboxConfig

stub = pytest.mark.xfail(reason="not implemented yet", strict=True)
"""For behaviour that's specified but still a stub. strict: an implemented one fails until unmarked."""


@pytest.fixture(autouse=True)
def no_user_config(tmp_path_factory, monkeypatch):
    """Keep a zenoh.json5 written by `expose-synq setup` out of the tests."""
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path_factory.mktemp("xdg")))


@pytest.fixture(scope="session")
def network():
    net = Network.open()
    yield net
    net.close()


@pytest.fixture
def share(network, tmp_path, request):
    """Factory: ``share(**ExposeConfig fields)`` serves ``tmp_path/ws`` over the test network.

    Exec runs unsandboxed unless ``sandbox=True``. ``ttl`` sets the ticket idle timeout.
    """
    shares = []
    name = re.sub(r"[^a-z0-9]+", "-", request.node.name.lower()).strip("-")

    def make(*, sandbox: bool = False, ttl: float | None = None, **fields):
        workspace = tmp_path / "ws"
        workspace.mkdir(exist_ok=True)
        fields.setdefault("exec", ExecConfig(sandbox=SandboxConfig(enabled=sandbox)))
        config = ExposeConfig(workspace=workspace, topic=f"expose/{name}-{len(shares)}", **fields)
        shares.append(network.share(config, ttl=ttl))
        return shares[-1]

    yield make
    for s in shares:
        s.close()
