"""Resolved runtime configuration, built by the CLI and read by everything else."""
import json
import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import json5
import zenoh

# Hidden unless --no-default-excludes is passed.
DEFAULT_EXCLUDES = (
    ".env",
    ".env.*",
    "*.pem",
    "*.key",
    "id_rsa*",
    "id_ed25519*",
    ".netrc",
)


@dataclass(frozen=True)
class SandboxConfig:
    enabled: bool = True
    allow_network: bool = False
    bind_ro: tuple[Path, ...] = ()
    bind: tuple[Path, ...] = ()


@dataclass(frozen=True)
class ExecConfig:
    sandbox: SandboxConfig = field(default_factory=SandboxConfig)
    pass_env: tuple[str, ...] = ()
    shell: str = "/bin/sh"
    timeout: float = 600.0
    max_procs: int = 4
    output_cap: int = 64 << 20
    """Bytes of output kept per process stream; older output is dropped."""


@dataclass(frozen=True)
class ExposeConfig:
    workspace: Path
    topic: str
    allow_write: bool = False
    allow_execute: bool = False
    exclude: tuple[str, ...] = DEFAULT_EXCLUDES
    readonly_paths: tuple[str, ...] = ()
    exec: ExecConfig = field(default_factory=ExecConfig)
    confirm: bool = False
    audit_log: Path | None = None


# ── Zenoh config ────────────────────────────────────────────────────────────
#
# Layers, later wins:
#   1. LOCKED_DOWN: no listening, connecting or scouting
#      (+ PUBLIC with --public: zenoh's normal scouting)
#   2. the config file (--zenoh-config / EXPOSE_SYNQ_ZENOH_CONFIG / ZENOH_CONFIG,
#      else ~/.config/expose-synq/zenoh.json5 if `expose-synq setup` wrote one),
#      then the layers servers handed us on enrollment (synq_expose.enroll)
#   3. EXPOSE_SYNQ_ZENOH__<SECTION>__<KEY>... environment variables
#   4. --zenoh KEY=VALUE flags
#   5. --connect / --listen
#
# Dicts merge key by key, anything else replaces. zenoh can't set nested keys
# under mode-dependent values (e.g. listen/endpoints/peer), so layers are
# merged here and each top-level section is written into zenoh whole.

LOCKED_DOWN: dict[str, Any] = {
    "mode": "peer",
    "listen": {"endpoints": []},
    "connect": {"endpoints": []},
    "scouting": {
        "multicast": {"enabled": False},
        "gossip": {"enabled": False},
    },
    "adminspace": {"enabled": False},
}

# Scouting alone is enough to be reached: we autoconnect out to the listeners
# other peers advertise, so --public doesn't need to open a listener.
PUBLIC: dict[str, Any] = {
    "scouting": {
        "multicast": {"enabled": True},
        "gossip": {"enabled": True},
    },
}

ENV_PREFIX = "EXPOSE_SYNQ_ZENOH__"

TOPIC_ENV = "EXPOSE_SYNQ_TOPIC"
"""The share client commands use when not given one: `run`'s --topic, and `file` paths without TOPIC:."""


class ZenohConfigError(ValueError):
    pass


def default_zenoh_config_path(environ: Mapping[str, str] = os.environ) -> Path:
    """Where ``expose-synq setup`` writes, and the config file used when none is given."""
    base = environ.get("XDG_CONFIG_HOME") or Path.home() / ".config"
    return Path(base) / "expose-synq" / "zenoh.json5"


def parse_value(raw: str) -> Any:
    """JSON5 if it parses (``false``, ``7447``, ``["tcp/…"]``), else the bare string (``eth1``)."""
    try:
        return json5.loads(raw)
    except ValueError:
        return raw


def nest(path: list[str], value: Any) -> dict[str, Any]:
    for key in reversed(path):
        value = {key: value}
    return value


def deep_merge(base: Any, over: Any) -> Any:
    if isinstance(base, Mapping) and isinstance(over, Mapping):
        merged = dict(base)
        for key, value in over.items():
            merged[key] = deep_merge(base.get(key), value)
        return merged
    return over


def env_layer(environ: Mapping[str, str]) -> dict[str, Any]:
    """``EXPOSE_SYNQ_ZENOH__SCOUTING__MULTICAST__INTERFACE=eth1`` → ``{"scouting": {"multicast": {"interface": "eth1"}}}``."""
    layer: dict[str, Any] = {}
    for name, raw in environ.items():
        if name.startswith(ENV_PREFIX):
            path = name.removeprefix(ENV_PREFIX).lower().split("__")
            layer = deep_merge(layer, nest(path, parse_value(raw)))
    return layer


def override_layer(spec: str) -> dict[str, Any]:
    """``scouting/multicast/interface=eth1`` → nested dict."""
    key, sep, raw = spec.partition("=")
    path = [p for p in key.strip().split("/") if p]
    if not sep or not path:
        raise ZenohConfigError(f"invalid --zenoh {spec!r}, expected KEY=VALUE (e.g. scouting/multicast/enabled=true)")
    return nest(path, parse_value(raw))


def build_zenoh_config(
    config_file: Path | None = None,
    overrides: list[str] = (),
    connect: list[str] = (),
    listen: list[str] = (),
    environ: Mapping[str, str] = os.environ,
    *,
    public: bool = False,
    enrolled: list[Mapping[str, Any]] = (),
) -> zenoh.Config:
    layers = [LOCKED_DOWN]
    if public:
        layers.append(PUBLIC)
    if config_file is not None:
        try:
            layers.append(json5.loads(config_file.read_text()))
        except ValueError as e:
            raise ZenohConfigError(f"{config_file}: {e}") from e
    layers += enrolled
    layers.append(env_layer(environ))
    layers += [override_layer(spec) for spec in overrides]
    if connect:
        layers.append({"connect": {"endpoints": list(connect)}})
    if listen:
        layers.append({"listen": {"endpoints": list(listen)}})

    merged: dict[str, Any] = {}
    for layer in layers:
        merged = deep_merge(merged, layer)

    config = zenoh.Config()
    for section, value in merged.items():
        try:
            current = json.loads(config.get_json(section))
            config.insert_json5(section, json.dumps(deep_merge(current, value)))
        except zenoh.ZError as e:
            raise ZenohConfigError(f"zenoh config {section!r}: {e}") from e
    return config


def reopen_session(session, config: zenoh.Config) -> None:
    """Swap the zenoh connection under an existing SpiriSynq ``Session`` for one opened with ``config``.

    zenoh can't reconfigure an open session, so this closes it and opens a
    new one, keeping the same ``Session`` object (and with it the type registry,
    codecs and ``current_session`` default). Must run before any object is
    created on the session, since its publishers and queryables die with the
    old connection.
    """
    if len(session.objects):
        raise RuntimeError("can't reopen a session that already has synced objects")
    session.zenoh_session.close()
    session.config = config
    session.zenoh_session = zenoh.open(config)
