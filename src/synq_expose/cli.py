import signal
import sys
import time
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console

from synq_expose.config import (
    DEFAULT_EXCLUDES,
    ExecConfig,
    ExposeConfig,
    SandboxConfig,
    ZenohConfigError,
    build_zenoh_config,
)

app = typer.Typer(add_completion=False, no_args_is_help=True)

# stdout is kept free for machine-readable output; status goes to stderr
console_err = Console(stderr=True)


def fail(message: str) -> typer.Exit:
    console_err.print(f"[red]Error:[/red] {message}")
    return typer.Exit(1)


@app.callback()
def main():
    """Expose local resources to remote agents over SpiriSynq."""


@app.command()
def share(
    # ── What ────────────────────────────────────────────────────────────
    workspace: Path = typer.Argument(
        Path("."), exists=True, file_okay=False, resolve_path=True,
        help="Folder to expose. Remote paths are confined to this root.",
    ),
    topic: Optional[str] = typer.Option(
        None, "--topic", "-t",
        help="Topic to publish under. Default: ./expose/<folder-name> (→ <hostname>/expose/<folder-name>).",
    ),

    # ── Capabilities (all off by default except read) ───────────────────
    allow_write: bool = typer.Option(
        False, "--allow-write", "-w",
        help="Allow creating, modifying, moving and deleting files in the workspace.",
    ),
    allow_execute: bool = typer.Option(
        False, "--allow-execute", "-x",
        help="Expose an exec endpoint. Commands run with the workspace as cwd.",
    ),
    sandbox: bool = typer.Option(
        True, "--sandbox/--no-sandbox",
        help="Run commands inside bubblewrap. --no-sandbox runs them as you, unconfined.",
    ),
    allow_network: bool = typer.Option(
        False, "--allow-network",
        help="Give sandboxed commands network access (default: --unshare-net).",
    ),

    # ── Visibility ──────────────────────────────────────────────────────
    exclude: list[str] = typer.Option(
        [], "--exclude", "-e",
        help="Glob of paths to hide entirely (unreadable, unlistable, not mounted in the sandbox). Repeatable.",
    ),
    default_excludes: bool = typer.Option(
        True, "--default-excludes/--no-default-excludes",
        help=f"Also hide likely secrets: {', '.join(DEFAULT_EXCLUDES)}.",
    ),
    readonly_paths: list[str] = typer.Option(
        [], "--read-only", "-r",
        help="Glob of paths that stay read-only even with --allow-write (e.g. .git/**). Repeatable.",
    ),

    # ── Sandbox tuning ──────────────────────────────────────────────────
    bind_ro: list[Path] = typer.Option(
        [], "--bind-ro", exists=True, resolve_path=True,
        help="Extra host path to mount read-only in the sandbox. Repeatable.",
    ),
    bind: list[Path] = typer.Option(
        [], "--bind", exists=True, resolve_path=True,
        help="Extra host path to mount read-write in the sandbox. Repeatable.",
    ),
    pass_env: list[str] = typer.Option(
        [], "--pass-env",
        help="Host env var to pass into executed commands. Repeatable. Default: a minimal PATH/HOME/LANG.",
    ),
    shell: str = typer.Option(
        "/bin/sh", "--shell",
        help="Shell used for string commands.",
    ),
    exec_timeout: float = typer.Option(
        600.0, "--exec-timeout",
        help="Kill a command after this many seconds (0 = never).",
    ),
    max_procs: int = typer.Option(
        4, "--max-procs", min=1,
        help="Maximum concurrent commands.",
    ),

    # ── Human in the loop ───────────────────────────────────────────────
    confirm: bool = typer.Option(
        False, "--confirm",
        help="Prompt on this terminal before each write or exec is allowed.",
    ),
    audit_log: Optional[Path] = typer.Option(
        None, "--audit-log", dir_okay=False, resolve_path=True,
        help="Append every remote operation as JSONL to this file.",
    ),

    # ── Zenoh connection ────────────────────────────────────────────────
    public: bool = typer.Option(
        False, "--public",
        help="Turn on zenoh's normal multicast + gossip scouting, so peers on the network find "
             "and connect to us. Opens no listener by itself (see --listen).",
    ),
    connect: list[str] = typer.Option(
        [], "--connect", "-c",
        help="Zenoh endpoint to connect to, e.g. tcp/relay.example.com:7447. Repeatable.",
    ),
    listen: list[str] = typer.Option(
        [], "--listen", "-l",
        help="Zenoh endpoint to listen on. Repeatable.",
    ),
    zenoh_config: Optional[Path] = typer.Option(
        None, "--zenoh-config", exists=True, dir_okay=False,
        envvar=["EXPOSE_SYNQ_ZENOH_CONFIG", "ZENOH_CONFIG"],
        help="Zenoh JSON5 config (scouting, TLS, auth, access_control), merged over a base that "
             "neither listens, connects nor scouts.",
    ),
    zenoh: list[str] = typer.Option(
        [], "--zenoh", "-Z",
        help="Set one zenoh config key, e.g. -Z scouting/multicast/interface=eth1. Repeatable. "
             "Env form: EXPOSE_SYNQ_ZENOH__SCOUTING__MULTICAST__INTERFACE=eth1. "
             "Precedence: base < config file < env < -Z < --connect/--listen.",
    ),
):
    """Expose WORKSPACE over SpiriSynq so a remote agent can work in it."""
    if not allow_execute:
        if not sandbox:
            raise fail("--no-sandbox only makes sense with --allow-execute")
        if allow_network or bind or bind_ro or pass_env:
            raise fail("sandbox/exec options require --allow-execute")
    if not sandbox and (allow_network or bind or bind_ro):
        raise fail("--allow-network/--bind/--bind-ro configure the sandbox; drop them or drop --no-sandbox")

    try:
        zenoh_cfg = build_zenoh_config(zenoh_config, zenoh, connect, listen, public=public)
    except ZenohConfigError as e:
        raise fail(str(e))

    config = ExposeConfig(
        workspace=workspace,
        topic=topic or f"./expose/{workspace.name}",
        allow_write=allow_write,
        allow_execute=allow_execute,
        exclude=(*(DEFAULT_EXCLUDES if default_excludes else ()), *exclude),
        readonly_paths=tuple(readonly_paths),
        exec=ExecConfig(
            sandbox=SandboxConfig(
                enabled=sandbox,
                allow_network=allow_network,
                bind_ro=tuple(bind_ro),
                bind=tuple(bind),
            ),
            pass_env=tuple(pass_env),
            shell=shell,
            timeout=exec_timeout,
            max_procs=max_procs,
        ),
        confirm=confirm,
        audit_log=audit_log,
    )

    if allow_execute and sandbox:
        from synq_expose.sandbox import SandboxUnavailable, find_bwrap
        try:
            find_bwrap()
        except SandboxUnavailable as e:
            raise fail(str(e))

    serve(config, zenoh_cfg)


def serve(config: ExposeConfig, zenoh_config) -> None:
    """Reopen the default session with our config, publish the workspace, and block until interrupted."""
    # Imported late: SpiriSynq opens its default zenoh session at import time
    # (#243), which we don't want for --help or argument errors.
    from SpiriSynq.session import current_session
    from synq_expose.config import reopen_session
    from synq_expose.tickets import TicketRegistry
    from synq_expose.workspace import build_objects

    session = current_session.get()
    reopen_session(session, zenoh_config)
    # Tickets live under the node's root, not the workspace topic.
    tickets = TicketRegistry(session.zenoh_session, session.base_topic)
    objects = build_objects(config, tickets)
    print_banner(config, objects[0].synq_absolute_path)
    # SIGTERM's default action kills us without running the finally below (or
    # SpiriSynq's shutdown hook); turn it into a normal exit instead.
    signal.signal(signal.SIGTERM, lambda signum, frame: sys.exit(0))
    try:
        while True:
            time.sleep(1)
            tickets.sweep()
    except KeyboardInterrupt:
        console_err.print("\n[dim]Stopping.[/dim]")
    finally:
        tickets.close()
        for obj in reversed(objects):
            obj.close()
        session.close()


def print_banner(config: ExposeConfig, path: str) -> None:
    caps = ["read"]
    if config.allow_write:
        caps.append("write")
    if config.allow_execute:
        caps.append("exec" + ("" if config.exec.sandbox.enabled else " [bold red](UNSANDBOXED)[/bold red]"))
    console_err.print(f"Exposing [bold]{config.workspace}[/bold] on [bold]{path}[/bold]")
    console_err.print(f"  capabilities: {', '.join(caps)}")
    if config.allow_execute and not config.exec.sandbox.enabled:
        console_err.print("  [bold red]commands run as you with full access to this machine[/bold red]")
    console_err.print("[dim]Ctrl+C to stop[/dim]")
