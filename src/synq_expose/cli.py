import functools
import inspect
import json
import os
import signal
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Optional

import click
import typer
from rich.console import Console

from synq_expose import completion
from synq_expose.config import (
    DEFAULT_EXCLUDES,
    ExecConfig,
    ExposeConfig,
    TOPIC_ENV,
    SandboxConfig,
    ZenohConfigError,
    build_zenoh_config,
    default_topic,
    default_zenoh_config_path,
)

# Completion scripts come from `expose-synq completion`; Typer's own install would replace our shell classes.
app = typer.Typer(add_completion=False, no_args_is_help=True)
completion.register()

# stdout is kept free for machine-readable output; status goes to stderr
console_err = Console(stderr=True)


def fail(message: str) -> typer.Exit:
    console_err.print(f"[red]Error:[/red] {message}")
    return typer.Exit(1)


# zenoh connection options, shared by every command that opens a session
Public = Annotated[bool, typer.Option(
    "--public",
    help="Use zenoh's own default config instead of the locked-down one: scouting and a "
         "listener, so peers on the network find and connect to us.",
)]
Connect = Annotated[list[str], typer.Option(
    "--connect", "-c",
    help="Zenoh endpoint to connect to, e.g. tcp/relay.example.com:7447. Repeatable. For share, a bare "
         "host or URL (hermes.example.com) is a server to enroll with: the first time, it asks for access "
         "and waits for a person to approve it.",
)]
Listen = Annotated[list[str], typer.Option(
    "--listen", "-l",
    help="Zenoh endpoint to listen on. Repeatable.",
)]
ZenohConfigFile = Annotated[Optional[Path], typer.Option(
    "--zenoh-config", exists=True, dir_okay=False,
    envvar=["EXPOSE_SYNQ_ZENOH_CONFIG", "ZENOH_CONFIG"],
    help="Zenoh JSON5 config (scouting, TLS, auth, access_control), merged over a base that "
         "neither listens, connects nor scouts. Default: the one `expose-synq setup` wrote, if any.",
)]
ZenohKeys = Annotated[list[str], typer.Option(
    "--zenoh", "-Z",
    help="Set one zenoh config key, e.g. -Z scouting/multicast/interface=eth1. Repeatable. "
         "Env form: EXPOSE_SYNQ_ZENOH__SCOUTING__MULTICAST__INTERFACE=eth1. "
         "Precedence: base < config file < env < -Z < --connect/--listen.",
)]


def zenoh_config_from_flags(config_file, overrides, connect, listen, public, enrolled=()):
    from synq_expose.enroll import is_server
    if servers := [c for c in connect if is_server(c)]:
        raise fail(f"{servers[0]} isn't a zenoh endpoint (e.g. tcp/{servers[0]}:7447); "
                   f"only share enrolls with a server")
    if config_file is None and default_zenoh_config_path().is_file():
        config_file = default_zenoh_config_path()
    try:
        return build_zenoh_config(config_file, overrides, connect, listen, public=public, enrolled=enrolled)
    except ZenohConfigError as e:
        raise fail(str(e))


def enroll_with_servers(connect: list[str]) -> tuple[list[str], list[dict]]:
    """Split ``--connect`` into zenoh endpoints and enrollment servers, and enroll with each server.

    Returns the endpoints, the servers' own included, and the servers' zenoh
    layers without their endpoints (lists replace when layers merge, so one
    server's would drop another's).
    """
    from synq_expose.enroll import EnrollError, enroll, is_server
    endpoints = [c for c in connect if not is_server(c)]
    servers = [c for c in connect if is_server(c)]
    if not servers:
        return endpoints, []
    # SpiriSynq opens its default session on import (#243); serve() replaces it.
    from SpiriSynq.session import current_session
    hostname = current_session.get().base_topic
    layers = []
    for server in servers:
        try:
            credential = enroll(
                server, hostname, default_zenoh_config_path().parent,
                on_code=lambda code, url: console_err.print(
                    f"Asking [bold]{url}[/bold] for access as [bold]{hostname}[/bold].\n"
                    f"  code: [bold]{code}[/bold]   approve it there if the code matches\n"
                    f"[dim]Waiting for approval (Ctrl+C to stop; running again resumes the request)[/dim]"),
                on_retry=lambda reason: console_err.print(f"[yellow]{reason}, retrying[/yellow]"),
            )
        except EnrollError as e:
            raise fail(str(e))
        console_err.print(f"Enrolled with [bold]{server}[/bold] as [bold]{hostname}[/bold] "
                          f"until {credential.expires:%Y-%m-%d %H:%M %Z}")
        layer = dict(credential.zenoh)
        connect_section = dict(layer.pop("connect", None) or {})
        endpoints += connect_section.pop("endpoints", [])
        if connect_section:
            layer["connect"] = connect_section
        layers.append(layer)
    return endpoints, layers


@app.callback()
def main():
    """Expose local resources to remote agents over SpiriSynq."""


@app.command()
def setup(
    output: Optional[Path] = typer.Option(
        None, "--output", "-o", dir_okay=False,
        help="Where to write the zenoh config. Default: ~/.config/expose-synq/zenoh.json5, "
             "which the other commands load when not given --zenoh-config.",
    ),
):
    """Write a zenoh config interactively: which interfaces to listen and scout on (ZeroTier networks
    by name), and how peers authenticate (password, TLS or mTLS). Certificates and the password file
    go next to it."""
    from synq_expose import wizard
    try:
        wizard.run(output)
    except RuntimeError as e:
        raise fail(str(e))


@app.command()
def share(
    # ── What ────────────────────────────────────────────────────────────
    workspace: Path = typer.Argument(
        Path("."), exists=True, file_okay=False, resolve_path=True,
        help="Folder to expose. Remote paths are confined to this root.",
    ),
    topic: Optional[str] = typer.Option(
        None, "--topic", "-t",
        help="Topic to publish at, under <hostname>. Default: the folder's absolute path "
             "(/home/me/proj → <hostname>/home/me/proj).",
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
    network: bool = typer.Option(
        True, "--network/--no-network",
        help="Give sandboxed commands network access. --no-network unshares it.",
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
        help="Host env var to pass into executed commands, on top of PATH, HOME and LANG. Repeatable.",
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
    public: Public = False,
    connect: Connect = [],
    listen: Listen = [],
    zenoh_config: ZenohConfigFile = None,
    zenoh: ZenohKeys = [],
):
    """Expose WORKSPACE over SpiriSynq so a remote agent can work in it."""
    if not allow_execute:
        if not sandbox:
            raise fail("--no-sandbox only makes sense with --allow-execute")
        if not network or bind or bind_ro or pass_env:
            raise fail("sandbox/exec options require --allow-execute")
    if not sandbox and (not network or bind or bind_ro):
        raise fail("--no-network/--bind/--bind-ro configure the sandbox; drop them or drop --no-sandbox")
    if topic is None:
        try:
            topic = default_topic(workspace)
        except ValueError as e:
            raise fail(str(e))

    endpoints, enrolled = enroll_with_servers(connect)
    zenoh_cfg = zenoh_config_from_flags(zenoh_config, zenoh, endpoints, listen, public, enrolled)

    config = ExposeConfig(
        workspace=workspace,
        topic=topic,
        allow_write=allow_write,
        allow_execute=allow_execute,
        exclude=(*(DEFAULT_EXCLUDES if default_excludes else ()), *exclude),
        readonly_paths=tuple(readonly_paths),
        exec=ExecConfig(
            sandbox=SandboxConfig(
                enabled=sandbox,
                allow_network=network,
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
    from synq_expose.workspace import build_objects, share_topic

    session = current_session.get()
    reopen_session(session, zenoh_config)
    # Tickets live under the node's root, not the workspace topic.
    tickets = TicketRegistry(session.zenoh_session, session.base_topic)
    objects = build_objects(config, tickets)
    print_banner(config, share_topic(objects))
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


@app.command()
def mount(
    topic: str = typer.Argument(
        ..., help="The share's topic, e.g. <hostname>/expose/<folder>, as `synq topic list --type FsRead` shows it (without /fs/read).",
        autocompletion=completion.complete_topic,
    ),
    mountpoint: Path = typer.Argument(..., exists=True, file_okay=False, resolve_path=True),
    timeout: float = typer.Option(10.0, "--timeout", help="Seconds to wait for the share to answer."),
    debug: bool = typer.Option(False, "--debug", help="Print every FUSE call (libfuse's -d)."),
    public: Public = False,
    connect: Connect = [],
    listen: Listen = [],
    zenoh_config: ZenohConfigFile = None,
    zenoh: ZenohKeys = [],
):
    """Mount the share at TOPIC on MOUNTPOINT with FUSE, until Ctrl+C or `fusermount3 -u MOUNTPOINT`."""
    zenoh_cfg = zenoh_config_from_flags(zenoh_config, zenoh, connect, listen, public)
    try:
        import synq_expose.mount as fuse_mount     # loads libfuse
    except OSError as e:
        raise fail(f"{e}: install libfuse 3, or set FUSE_LIBRARY_PATH to libfuse3.so")

    from SpiriSynq.session import current_session
    from synq_expose.config import reopen_session
    from synq_expose.fs import FsRead, FsWrite
    from synq_expose.workspace import DiscoveryError, discover

    topic = topic.rstrip("/")
    session = current_session.get()
    reopen_session(session, zenoh_cfg)
    mirrors = []
    try:
        try:
            granted = discover(session, topic, timeout)
        except DiscoveryError as e:
            raise fail(str(e))
        mirrors.append(FsRead.from_topic(granted["FsRead"]))
        writable = "FsWrite" in granted
        if writable:
            mirrors.append(FsWrite.from_topic(granted["FsWrite"]))
        operations = fuse_mount.SynqFS(session.zenoh_session, mirrors[0], mirrors[1] if writable else None)
        console_err.print(f"Mounting [bold]{topic}[/bold] on [bold]{mountpoint}[/bold]"
                          f" ({'read/write' if writable else 'read-only'})")
        console_err.print(f"[dim]Ctrl+C or fusermount3 -u {mountpoint} to unmount[/dim]")
        fuse_mount.mount(operations, str(mountpoint), read_only=not writable, fsname=topic, debug=debug)
    finally:
        for mirror in mirrors:
            mirror.close()
        session.close()


@app.command(context_settings={"allow_interspersed_args": False, "ignore_unknown_options": True})
def run(
    topic: str = typer.Option(
        ..., "--topic", envvar=TOPIC_ENV, show_envvar=True,
        help="The share's topic, e.g. <hostname>/expose/<folder>, as `synq topic list --type FsRead` shows it (without /fs/read).",
        autocompletion=completion.complete_topic,
    ),
    command: list[str] = typer.Argument(
        ..., help="The command and its arguments, run as argv in the share's workspace. Options after it are its own.",
    ),
    tty: Optional[bool] = typer.Option(
        None, "--tty/--no-tty", "-t/-T",
        help="Run it on a pty, interactively. Default: when stdin and stdout are both terminals.",
    ),
    no_stdin: bool = typer.Option(False, "--no-stdin", "-n", help="Don't forward stdin (without a pty)."),
    timeout: float = typer.Option(10.0, "--timeout", help="Seconds to wait for the share to answer."),
    public: Public = False,
    connect: Connect = [],
    listen: Listen = [],
    zenoh_config: ZenohConfigFile = None,
    zenoh: ZenohKeys = [],
):
    """Run COMMAND on the share at --topic, attached to this terminal, until it exits. Exits with its status.

    The share needs --allow-execute. Like ssh: with a terminal on both ends the command gets a pty and
    every keystroke goes to it, Ctrl-C included; otherwise stdin, stdout and stderr are piped through.
    Options go before COMMAND: everything from it on is the command's.
    """
    quiet_logs()
    zenoh_cfg = zenoh_config_from_flags(zenoh_config, zenoh, connect, listen, public)

    from synq_expose import client
    from synq_expose.run import LOST, default_tty, run_command

    status = 0
    with connected(topic.rstrip("/"), ("Shell",), Conn(timeout, zenoh_cfg)) as (z, mirrors):
        try:
            status = run_command(z, mirrors["Shell"], command,
                                 tty=default_tty() if tty is None else tty, forward_stdin=not no_stdin)
        except client.TicketError as e:
            console_err.print(f"[red]Lost the share:[/red] {e}")
            status = LOST
    raise typer.Exit(status)


@dataclass
class Conn:
    """How to reach a share: the connection options every client command takes."""
    timeout: float
    zenoh_config: object


_MISSING = {
    "FsWrite": "is read-only (share it with --allow-write)",
    "Shell": "doesn't allow running commands (share it with --allow-execute)",
}


def quiet_logs() -> None:
    """Only warnings and errors from loguru (SpiriSynq's own INFO and DEBUG), so a client command's stderr is its own."""
    from loguru import logger
    logger.remove()
    logger.add(sys.stderr, level="WARNING")


@contextmanager
def connected(topic: str, need: tuple[str, ...], conn: Conn, error_status: int = 1):
    """Find the share at ``topic`` and yield ``(zenoh session, {class name: mirror})`` for the classes in ``need``.

    A remote error (an RPC's or a ticket's) is printed and exits with ``error_status``.
    """
    from SpiriSynq.remote_callables import RpcException
    from SpiriSynq.session import current_session
    from synq_expose.client import TicketError
    from synq_expose.config import reopen_session
    from synq_expose.fs import FsRead, FsWrite
    from synq_expose.shell import Shell
    from synq_expose.workspace import DiscoveryError, discover

    classes = {"FsRead": FsRead, "FsWrite": FsWrite, "Shell": Shell}
    session = current_session.get()
    reopen_session(session, conn.zenoh_config)
    mirrors = {}
    try:
        try:
            granted = discover(session, topic, conn.timeout)
        except DiscoveryError as e:
            raise fail(str(e))
        for name in need:
            if name not in granted:
                raise fail(f"{topic} {_MISSING[name]}")
            mirrors[name] = classes[name].from_topic(granted[name])
        try:
            yield session.zenoh_session, mirrors
        except (RpcException, TicketError, OSError) as e:
            console_err.print(f"[red]Error:[/red] {e}")
            raise typer.Exit(error_status)
    finally:
        for mirror in mirrors.values():
            mirror.close()
        session.close()


_CONN_PARAMS = [inspect.Parameter(name, inspect.Parameter.KEYWORD_ONLY, default=default, annotation=annotation)
                for name, annotation, default in [
                    ("timeout", Annotated[float, typer.Option("--timeout", help="Seconds to wait for the share to answer.")], 10.0),
                    ("public", Public, False),
                    ("connect", Connect, []),
                    ("listen", Listen, []),
                    ("zenoh_config", ZenohConfigFile, None),
                    ("zenoh", ZenohKeys, []),
                ]]


def share_options(fn):
    """Give a command the connection options; it gets them as one ``conn: Conn`` argument."""
    signature = inspect.signature(fn)
    params = [p for p in signature.parameters.values() if p.name != "conn"] + _CONN_PARAMS

    @functools.wraps(fn)
    def wrapper(**kwargs):
        quiet_logs()
        timeout = kwargs.pop("timeout")
        zenoh_cfg = zenoh_config_from_flags(kwargs.pop("zenoh_config"), kwargs.pop("zenoh"), kwargs.pop("connect"),
                                            kwargs.pop("listen"), kwargs.pop("public"))
        return fn(conn=Conn(timeout, zenoh_cfg), **kwargs)

    wrapper.__signature__ = signature.replace(parameters=params)
    wrapper.__annotations__ = {p.name: p.annotation for p in params}
    return wrapper


@app.command("completion")
def completion_script(shell: completion.Shell):
    """Print the tab-completion script for SHELL. It completes share topics, and the paths after TOPIC:.

    zsh: expose-synq completion zsh > ~/.zfunc/_expose-synq, with ~/.zfunc in $fpath before compinit.
    bash: expose-synq completion bash > ~/.local/share/bash-completion/completions/expose-synq,
    or source the script from ~/.bashrc. Regenerate it after upgrading.
    """
    typer.echo(completion.script(shell, "expose-synq"), nl=False)


@app.command("_complete-server", hidden=True)
def complete_server(
    idle: float = typer.Option(600.0, "--idle", help="Exit after this many seconds without a Tab."),
    zenoh_config: ZenohConfigFile = None,
):
    """Answer tab completion over a socket, with a zenoh session kept open. Started by the completion script."""
    from synq_expose.complete_server import serve
    quiet_logs()
    if zenoh_config is None and default_zenoh_config_path().is_file():
        zenoh_config = default_zenoh_config_path()
    serve(zenoh_config_from_flags(zenoh_config, [], [], [], False),
          watch=[zenoh_config or default_zenoh_config_path()], idle=idle)


# ── expose-synq file ────────────────────────────────────────────────────

file_app = typer.Typer(
    no_args_is_help=True,
    help=f"Small commands on a share's files, one per remote method. A remote path is TOPIC:PATH, as scp "
         f"writes host:path; without a TOPIC: the topic comes from {TOPIC_ENV}. Write :PATH for a path "
         f"holding a colon.",
)
app.add_typer(file_app, name="file")

def remote_argument(help: str):
    return typer.Argument(metavar="[TOPIC:]PATH", help=help, autocompletion=completion.complete_remote)


Remote = Annotated[str, remote_argument("A path on the share.")]
Json = Annotated[bool, typer.Option("--json", help="One JSON object per line, as the method returns it.")]


def remote(arg: str) -> tuple[str, str]:
    from synq_expose.files import NoTopic, split
    try:
        return split(arg, os.environ.get(TOPIC_ENV))
    except NoTopic as e:
        raise fail(str(e))


def stdout_bytes():
    return click.get_binary_stream("stdout")


@file_app.command("ls")
@share_options
def file_ls(path: Annotated[str, remote_argument("A directory on the share.")] = ".",
            as_json: Json = False, *, conn: Conn):
    """List a directory: type and mode, size, mtime and name, by name."""
    from synq_expose.files import ls_line
    topic, path = remote(path)
    with connected(topic, ("FsRead",), conn) as (z, m):
        for entry in m["FsRead"].list_dir(path):
            typer.echo(json.dumps(entry) if as_json else ls_line(entry))


@file_app.command("stat")
@share_options
def file_stat(path: Remote, as_json: Json = False, *, conn: Conn):
    """lstat a path: type, size, mode and mtime."""
    from synq_expose.files import stat_lines
    topic, path = remote(path)
    with connected(topic, ("FsRead",), conn) as (z, m):
        st = m["FsRead"].stat(path)
        typer.echo(json.dumps(st) if as_json else "\n".join(stat_lines(st)))


@file_app.command("hash")
@share_options
def file_hash(path: Remote, algorithm: str = typer.Option("sha256", "--algorithm", "-a", help="Any hashlib algorithm."),
              *, conn: Conn):
    """Print a file's digest, as <algorithm>:<hex>."""
    topic, path = remote(path)
    with connected(topic, ("FsRead",), conn) as (z, m):
        typer.echo(m["FsRead"].hash(path, algorithm))


@file_app.command("glob")
@share_options
def file_glob(pattern: str = typer.Argument(..., help="Relative to PATH; * within a segment, ** across them."),
              path: Annotated[str, remote_argument("Where to search.")] = ".",
              *, conn: Conn):
    """Print the workspace-relative paths under PATH matching PATTERN, one per line."""
    topic, path = remote(path)
    with connected(topic, ("FsRead",), conn) as (z, m):
        for found in m["FsRead"].glob(path, pattern):
            typer.echo(found)


@file_app.command("grep")
@share_options
def file_grep(pattern: str = typer.Argument(..., help="A Python regex."),
              path: Annotated[str, remote_argument("A file or a directory to search.")] = ".",
              as_json: Json = False, *, conn: Conn):
    """Print path:line:text for each matching line, as grep -rn. Exits 1 if nothing matched, 2 on an error."""
    from synq_expose.files import grep_line
    topic, path = remote(path)
    matched = False
    with connected(topic, ("FsRead",), conn, error_status=2) as (z, m):
        for match in m["FsRead"].grep(path, pattern):
            matched = True
            typer.echo(json.dumps(match) if as_json else grep_line(match))
    raise typer.Exit(0 if matched else 1)


@file_app.command("pull")
@share_options
def file_pull(source: Remote,
              dest: str = typer.Argument("-", metavar="[LOCAL]", help="Local file to write. Default: stdout."),
              start: int = typer.Option(0, "--start", help="First byte to read."),
              end: Optional[int] = typer.Option(None, "--end", help="Byte to stop before. Default: end of file."),
              *, conn: Conn):
    """Copy a remote file to LOCAL, or to stdout."""
    from synq_expose.files import pull
    topic, path = remote(source)
    out = stdout_bytes() if dest == "-" else lambda: open(dest, "wb")
    with connected(topic, ("FsRead",), conn) as (z, m):
        pull(z, m["FsRead"], path, out, start, end)


@file_app.command("push")
@share_options
def file_push(source: str = typer.Argument(..., metavar="LOCAL", help="Local file to send, or - for stdin."),
              dest: Remote = ...,
              append: bool = typer.Option(False, "--append", help="Append, as >> does, instead of truncating."),
              exclusive: bool = typer.Option(False, "--exclusive", help="Fail if the remote file exists (O_EXCL)."),
              *, conn: Conn):
    """Copy LOCAL, or stdin, to a remote file, creating it or truncating it first (unless --append)."""
    from synq_expose.files import local_chunks, push
    topic, path = remote(dest)
    try:
        f = click.get_binary_stream("stdin") if source == "-" else open(source, "rb")
    except OSError as e:
        raise fail(str(e))
    with f, connected(topic, ("FsWrite",), conn) as (z, m):
        push(z, m["FsWrite"], path, local_chunks(f), append=append, exclusive=exclusive)


@file_app.command("edit")
@share_options
def file_edit(path: Remote,
              texts: Annotated[Optional[list[str]], typer.Argument(
                  metavar="[OLD] [NEW]", help="The text to replace and its replacement, unless given as files.")] = None,
              old_file: Optional[str] = typer.Option(None, "--old-file", help="Read OLD from this file (- for stdin)."),
              new_file: Optional[str] = typer.Option(None, "--new-file", help="Read NEW from this file (- for stdin)."),
              *, conn: Conn):
    """Replace the one occurrence of OLD with NEW. Fails if OLD matches no times or more than once."""
    from synq_expose.files import read_text
    texts = list(texts or [])
    if len(texts) != (old_file is None) + (new_file is None):
        raise fail("give OLD and NEW, as arguments or with --old-file / --new-file")
    if old_file == new_file == "-":
        raise fail("only one of --old-file and --new-file can be stdin")
    stdin = click.get_binary_stream("stdin")
    try:
        old = texts.pop(0) if old_file is None else read_text(old_file, stdin)
        new = texts.pop(0) if new_file is None else read_text(new_file, stdin)
    except OSError as e:
        raise fail(str(e))
    topic, path = remote(path)
    with connected(topic, ("FsWrite",), conn) as (z, m):
        m["FsWrite"].edit_file(path, old, new)


@file_app.command("mkdir")
@share_options
def file_mkdir(path: Remote, parents: bool = typer.Option(False, "--parents", "-p", help="mkdir -p."), *, conn: Conn):
    """Create a directory."""
    topic, path = remote(path)
    with connected(topic, ("FsWrite",), conn) as (z, m):
        m["FsWrite"].mkdir(path, parents)


@file_app.command("rm")
@share_options
def file_rm(path: Remote, recursive: bool = typer.Option(False, "--recursive", "-r", help="rm -r."), *, conn: Conn):
    """Remove a file, a symlink or an empty directory; with -r, a directory tree."""
    topic, path = remote(path)
    with connected(topic, ("FsWrite",), conn) as (z, m):
        m["FsWrite"].delete(path, recursive)


@file_app.command("mv")
@share_options
def file_mv(source: Remote, dest: Remote, *, conn: Conn):
    """rename(2) on the share. Both paths must be on the same share."""
    topic, path = remote(source)
    dest_topic, to = remote(dest)
    if dest_topic != topic:
        raise fail(f"can't move between shares ({topic} and {dest_topic}): pull and push instead")
    with connected(topic, ("FsWrite",), conn) as (z, m):
        m["FsWrite"].rename(path, to)


@file_app.command("chmod")
@share_options
def file_chmod(mode: str = typer.Argument(..., help="Octal permission bits, e.g. 644."), path: Remote = ...,
               *, conn: Conn):
    """Set a path's permission bits."""
    from synq_expose.files import parse_mode
    try:
        bits = parse_mode(mode)
    except ValueError as e:
        raise fail(str(e))
    topic, path = remote(path)
    with connected(topic, ("FsWrite",), conn) as (z, m):
        m["FsWrite"].chmod(path, bits)

