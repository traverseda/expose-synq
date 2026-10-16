import json

import pytest

from synq_expose.config import ZenohConfigError, build_zenoh_config


def get(config, key):
    return json.loads(config.get_json(key))


def test_base_is_locked_down():
    config = build_zenoh_config(environ={})
    assert get(config, "listen/endpoints") == []
    assert get(config, "connect/endpoints") == []
    assert get(config, "scouting/multicast/enabled") is False
    assert get(config, "scouting/gossip/enabled") is False


def test_file_merges_over_base(tmp_path):
    path = tmp_path / "zenoh.json5"
    path.write_text("{ scouting: { multicast: { enabled: true, interface: 'eth1' } } }")
    config = build_zenoh_config(path, environ={})
    assert get(config, "scouting/multicast/enabled") is True
    assert get(config, "scouting/multicast/interface") == "eth1"
    # untouched sections keep the locked-down base
    assert get(config, "scouting/gossip/enabled") is False
    assert get(config, "listen/endpoints") == []


def test_mode_dependent_values_from_file(tmp_path):
    path = tmp_path / "zenoh.json5"
    path.write_text("{ listen: { endpoints: { peer: ['tcp/10.0.0.5:7447'] } } }")
    config = build_zenoh_config(path, environ={})
    assert get(config, "listen/endpoints")["peer"] == ["tcp/10.0.0.5:7447"]


def test_precedence(tmp_path):
    path = tmp_path / "zenoh.json5"
    path.write_text("{ scouting: { multicast: { interface: 'from-file' } } }")
    environ = {
        "EXPOSE_SYNQ_ZENOH__SCOUTING__MULTICAST__INTERFACE": "from-env",
        "EXPOSE_SYNQ_ZENOH__SCOUTING__MULTICAST__ENABLED": "true",
    }
    config = build_zenoh_config(path, environ=environ)
    assert get(config, "scouting/multicast/interface") == "from-env"
    assert get(config, "scouting/multicast/enabled") is True

    config = build_zenoh_config(path, ["scouting/multicast/interface=from-flag"], environ=environ)
    assert get(config, "scouting/multicast/interface") == "from-flag"


def test_connect_listen_win():
    environ = {"EXPOSE_SYNQ_ZENOH__CONNECT__ENDPOINTS": '["tcp/env:7447"]'}
    config = build_zenoh_config(connect=["tcp/flag:7447"], listen=["tcp/127.0.0.1:0"], environ=environ)
    assert get(config, "connect/endpoints") == ["tcp/flag:7447"]
    assert get(config, "listen/endpoints") == ["tcp/127.0.0.1:0"]


def test_unknown_key_is_an_error():
    with pytest.raises(ZenohConfigError):
        build_zenoh_config(overrides=["scouting/bogus=1"], environ={})


def test_bad_override_syntax():
    with pytest.raises(ZenohConfigError):
        build_zenoh_config(overrides=["no-equals-sign"], environ={})


def test_public_enables_scouting_but_not_listening():
    config = build_zenoh_config(environ={}, public=True)
    assert get(config, "scouting/multicast/enabled") is True
    assert get(config, "scouting/gossip/enabled") is True
    assert get(config, "listen/endpoints") == []


def test_public_can_be_narrowed_by_later_layers():
    environ = {"EXPOSE_SYNQ_ZENOH__SCOUTING__MULTICAST__INTERFACE": "eth1"}
    config = build_zenoh_config(environ=environ, public=True)
    assert get(config, "scouting/multicast/enabled") is True
    assert get(config, "scouting/multicast/interface") == "eth1"
