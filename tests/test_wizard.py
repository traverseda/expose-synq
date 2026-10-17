import json
import socket
import stat
import time

import json5
import pytest
import zenoh
from typer.testing import CliRunner

from synq_expose import interfaces as netif
from synq_expose import wizard
from synq_expose.cli import app
from synq_expose.config import build_zenoh_config
from synq_expose.wizard import Password, Plan, TlsFiles, build_config


def get(config, key):
    return json.loads(config.get_json(key))


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def test_plain_config():
    config = build_config(Plan(listen=("192.168.193.68",), scout_interface="zt6jyrztxd",
                               connect=("192.168.193.10:7447", "quic/10.0.0.1:7447")))
    assert config["listen"]["endpoints"] == ["tcp/192.168.193.68:0"]
    assert config["connect"]["endpoints"] == ["tcp/192.168.193.10:7447", "quic/10.0.0.1:7447"]
    assert config["scouting"]["multicast"] == {"enabled": True, "interface": "zt6jyrztxd"}
    assert "transport" not in config


def test_no_scouting_and_ipv6():
    config = build_config(Plan(listen=("fd00::68",), port=7447))
    assert config["listen"]["endpoints"] == ["tcp/[fd00::68]:7447"]
    assert config["scouting"]["multicast"] == {"enabled": False}


def test_tls_needs_certificates():
    with pytest.raises(ValueError):
        build_config(Plan(listen=("127.0.0.1",), protocol="tls"))
    with pytest.raises(ValueError):
        build_config(Plan(listen=("127.0.0.1",), mtls=True))


def test_dictionary_keeps_other_users(tmp_path):
    path = tmp_path / "usrpwd.txt"
    path.write_text("alice:one\nme:old\n")
    wizard.write_dictionary(path, "me", "new")
    assert path.read_text() == "alice:one\nme:new\n"
    assert stat.S_IMODE(path.stat().st_mode) == 0o600


@pytest.fixture
def certs(tmp_path):
    """Two nodes signed by one CA, and a rogue signed by another."""
    def node(ca, name):
        wizard.issue_cert(ca / "ca.crt", ca / "ca.key", ca / f"{name}.crt", ca / f"{name}.key", name, ("127.0.0.1",))
        return TlsFiles(ca / "ca.crt", ca / f"{name}.crt", ca / f"{name}.key")

    ours, theirs = tmp_path / "ours", tmp_path / "theirs"
    wizard.generate_ca(ours / "ca.crt", ours / "ca.key")
    wizard.generate_ca(theirs / "ca.crt", theirs / "ca.key")
    assert stat.S_IMODE((ours / "ca.key").stat().st_mode) == 0o600
    return {"a": node(ours, "a"), "b": node(ours, "b"), "rogue": node(theirs, "rogue")}


def password(tmp_path, name, pw):
    path = tmp_path / f"{name}-usrpwd.txt"
    wizard.write_dictionary(path, "expose-synq", pw)
    return Password("expose-synq", pw, path)


def connects(tmp_path, listener: Plan, connector: Plan, port: int) -> bool:
    sessions = []
    try:
        for name, plan in (("listener", listener), ("connector", connector)):
            path = tmp_path / f"{name}.json5"
            path.write_text(wizard.render(build_config(plan)))
            sessions.append(zenoh.open(build_zenoh_config(path, environ={})))
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            if list(sessions[1].info.peers_zid()):
                return True
            time.sleep(0.1)
        return False
    finally:
        for s in sessions:
            s.close()


@pytest.mark.parametrize("protocol", ["tls", "quic"])
@pytest.mark.parametrize("case", ["ok", "rogue-cert", "wrong-password"])
def test_mtls_and_password(tmp_path, certs, protocol, case):
    port = free_port()
    pw = password(tmp_path, "a", "shared")
    other_pw = password(tmp_path, "b", "wrong" if case == "wrong-password" else "shared")
    listener = Plan(listen=("127.0.0.1",), port=port, protocol=protocol, tls=certs["a"], mtls=True, password=pw)
    connector = Plan(listen=("127.0.0.1",), connect=(f"127.0.0.1:{port}",), protocol=protocol,
                     tls=certs["rogue" if case == "rogue-cert" else "b"], mtls=True, password=other_pw)
    assert connects(tmp_path, listener, connector, port) is (case == "ok")


def test_password_over_tcp(tmp_path):
    port = free_port()
    listener = Plan(listen=("127.0.0.1",), port=port, password=password(tmp_path, "a", "shared"))
    good = Plan(listen=("127.0.0.1",), connect=(f"127.0.0.1:{port}",), password=password(tmp_path, "b", "shared"))
    unauthenticated = Plan(listen=("127.0.0.1",), connect=(f"127.0.0.1:{port}",))
    assert connects(tmp_path, listener, good, port)
    assert not connects(tmp_path, listener, unauthenticated, port)


@pytest.fixture
def fake_interfaces(monkeypatch):
    found = [netif.Interface("wlan0", ("192.168.0.102",)), netif.Interface("zt6jyrztxd", ("192.168.193.68",))]
    networks = [netif.ZeroTierNetwork("8056c2e21c000001", "homelab", "zt6jyrztxd")]
    monkeypatch.setattr(netif, "list_interfaces", lambda: found)
    monkeypatch.setattr(netif, "zerotier_networks", lambda: networks)


def test_setup_command(tmp_path, fake_interfaces):
    output = tmp_path / "cfg" / "zenoh.json5"
    answers = [
        "",                 # interfaces: default is the ZeroTier one
        "",                 # port 0
        "",                 # scout on it: yes
        "192.168.193.10:7447",
        "mtls+password",
        "quic",
        "",                 # certificates: new
        "",                 # username
        "",                 # password: generate
    ]
    result = CliRunner().invoke(app, ["setup", "-o", str(output)], input="\n".join(answers) + "\n")
    assert result.exit_code == 0, result.output
    assert "ZeroTier homelab" in result.output

    config = json5.loads(output.read_text())
    assert config["listen"]["endpoints"] == ["quic/192.168.193.68:0"]
    assert config["connect"]["endpoints"] == ["quic/192.168.193.10:7447"]
    assert config["scouting"]["multicast"]["interface"] == "zt6jyrztxd"
    tls = config["transport"]["link"]["tls"]
    assert tls["enable_mtls"] is True
    assert tls["root_ca_certificate"] == str(output.parent / "tls" / "ca.crt")
    usrpwd = config["transport"]["auth"]["usrpwd"]
    assert usrpwd["password"] in result.output      # the generated one is shown once
    assert (output.parent / "usrpwd.txt").read_text() == f"expose-synq:{usrpwd['password']}\n"
    assert stat.S_IMODE(output.stat().st_mode) == 0o600

    # a second run on the same machine offers the existing CA
    result = CliRunner().invoke(app, ["setup", "-o", str(output)],
                                input="\n".join(["y", "", "", "", "", "mtls", "tls", "", "", ""]) + "\n")
    assert result.exit_code == 0, result.output
    config = json5.loads(output.read_text())
    assert config["transport"]["link"]["tls"]["root_ca_certificate"] == str(output.parent / "tls" / "ca.crt")
    assert "auth" not in config["transport"]


def test_default_config_is_loaded(tmp_path, monkeypatch, fake_interfaces):
    """share/mount/run pick up what setup wrote when not given --zenoh-config."""
    from synq_expose import cli

    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    result = CliRunner().invoke(app, ["setup"], input="\n".join(["", "", "n", "", "none"]) + "\n")
    assert result.exit_code == 0, result.output
    config = cli.zenoh_config_from_flags(None, [], [], [], False)
    assert get(config, "listen/endpoints") == ["tcp/192.168.193.68:0"]
    assert get(config, "scouting/gossip/enabled") is True
