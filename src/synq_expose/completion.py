"""Tab completion: share topics, and the paths in a share after ``TOPIC:``.

A Tab press can't wait for Python to start, let alone for a fresh zenoh
session to learn what's out there. So the completion script asks a server
instead: ``expose-synq _complete-server`` (complete_server.py), one warm
process with a zenoh session open, that runs this same completion code and
exits after ``IDLE`` seconds without a Tab. The shell talks to it itself, so
no Python starts: zsh over a Unix socket (``zsh/net/socket``), bash over
TCP on 127.0.0.1 with a token, as bash can't open a Unix socket.

When the server isn't running the script falls back to running
``expose-synq`` the way Typer's does, which answers from the topics the
server last wrote to ``~/.cache/expose-synq/topics`` (no paths) and starts
the server for the next Tab.

Typer's bash and zsh classes are replaced, for ``TOPIC:PATH``:

- bash splits words at ``:``, so ``host/proj:sr`` reaches the completion
  function as ``host/proj`` ``:`` ``sr``. Ours hands Click the raw line
  instead and trims each answer back to bash's last word, as scp's
  completion does.
- zsh adds a space after a single match. A candidate ending in ``:`` or
  ``/`` is waiting for more, so it gets none (bash: ``compopt -o nospace``).
"""
import os
import shlex
import socket
import subprocess
import sys
import time
from collections.abc import Mapping
from enum import StrEnum
from pathlib import Path

# Typer's shell classes and its vendored Click: private, but what --install-completion itself uses.
from typer import _completion_classes as typer_shells
from typer._click.shell_completion import add_completion_class, get_completion_class, split_arg_string

PROG = "expose-synq"
COMPLETE_VAR = "_EXPOSE_SYNQ_COMPLETE"
PROTOCOL = "1"
"""First field of a request. A server that doesn't speak it closes the connection, and the shell falls back."""

START_GAP = 5.0
"""Seconds after starting a server before the fallback starts another, so a burst of Tabs starts one."""


def cache_path(environ: Mapping[str, str] = os.environ) -> Path:
    base = environ.get("XDG_CACHE_HOME") or Path.home() / ".cache"
    return Path(base) / "expose-synq" / "topics"


def runtime_dir(environ: Mapping[str, str] = os.environ) -> Path:
    """Where the server's socket lives: ``$XDG_RUNTIME_DIR/expose-synq``, else next to the cache.

    The scripts work it out the same way, as
    ``${XDG_RUNTIME_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}}/expose-synq``.
    """
    base = environ.get("XDG_RUNTIME_DIR")
    return Path(base) / "expose-synq" if base else cache_path(environ).parent


def read_topics(path: Path) -> list[str]:
    try:
        return path.read_text().splitlines()
    except FileNotFoundError:
        return []


def write_topics(path: Path, topics) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}")
    tmp.write_text("".join(f"{topic}\n" for topic in sorted(topics)))
    os.replace(tmp, path)


# ── What the callbacks complete from ────────────────────────────────────

class CachedSource:
    """Without the server: the topics it last wrote, and no paths."""

    def topics(self) -> list[str]:
        return read_topics(cache_path())

    def default_topic(self) -> str | None:
        return os.environ.get("EXPOSE_SYNQ_TOPIC") or None

    def paths(self, topic: str, partial: str) -> list[str]:
        return []


source = CachedSource()
"""The server swaps in its own (complete_server.LiveSource)."""


def complete_topic(incomplete: str) -> list[str]:
    """Autocompletion for a bare TOPIC."""
    return [t for t in source.topics() if t.startswith(incomplete)]


def complete_remote(incomplete: str) -> list[str]:
    """Autocompletion for [TOPIC:]PATH, split as files.split does.

    Without a colon it's a topic, and with $EXPOSE_SYNQ_TOPIC set also a
    path in that topic. A topic holding a ``:`` is never offered as
    ``TOPIC:``, since ``TOPIC:PATH`` splits at the first one.
    """
    default = source.default_topic()
    topic, colon, path = incomplete.partition(":")
    if incomplete.startswith("./"):
        return source.paths(default, incomplete) if default else []
    if colon:
        topic = topic.rstrip("/") or default
        return [f"{incomplete[:-len(path) or None]}{p}" for p in source.paths(topic, path)] if topic else []
    found = [f"{t}:" for t in source.topics() if ":" not in t and t.startswith(incomplete)]
    return found + (source.paths(default, incomplete) if default else [])


# ── Starting the server ─────────────────────────────────────────────────

def server_running(run: Path) -> bool:
    with socket.socket(socket.AF_UNIX) as s:
        try:
            s.connect(str(run / "complete.sock"))
        except OSError:
            return False
    return True


def start_server(environ: Mapping[str, str] = os.environ) -> None:
    """Start ``_complete-server`` detached, unless it's running or was just started."""
    run = runtime_dir(environ)
    stamp = run / "complete.started"
    try:
        if time.time() - stamp.stat().st_mtime < START_GAP:
            return
    except FileNotFoundError:
        run.mkdir(mode=0o700, parents=True, exist_ok=True)
    if server_running(run):
        return
    stamp.touch()
    subprocess.Popen(
        [sys.executable, "-m", "synq_expose", "_complete-server"],
        stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        cwd="/", start_new_session=True,
    )


# ── Shells ──────────────────────────────────────────────────────────────

RUNTIME_DIR_SH = "${XDG_RUNTIME_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}}/expose-synq"

BASH = """\
%(complete_func)s() {
    local line=${COMP_LINE:0:COMP_POINT} cur=${COMP_WORDS[COMP_CWORD]}
    local dir=""" + RUNTIME_DIR_SH + """ port token fd reply
    COMPREPLY=()
    # A remote path: offer nothing rather than local files.
    [[ ${line##*[[:space:]]} == *:* ]] && compopt +o default
    if read -r port token 2>/dev/null <"$dir/complete.tcp" &&
       { exec {fd}<>"/dev/tcp/127.0.0.1/$port"; } 2>/dev/null; then
        printf '""" + PROTOCOL + """\\t%%s\\tbash\\t%%s\\t%%s\\t%%s\\n' "$token" "$EXPOSE_SYNQ_TOPIC" "$cur" "$line" >&$fd
        while IFS= read -r -t 2 -u $fd reply; do COMPREPLY+=("$reply"); done
        exec {fd}>&-
    else
        local IFS=$'\\n'
        COMPREPLY=( $(env COMP_LINE="$line" COMP_CUR="$cur" %(autocomplete_var)s=complete_bash "$1") )
    fi
    if [[ ${#COMPREPLY[@]} == 1 && ${COMPREPLY[0]} == *[:/] ]]; then
        compopt -o nospace
    fi
    return 0
}

complete -o default -F %(complete_func)s %(prog_name)s
"""

ZSH = """\
#compdef %(prog_name)s

%(complete_func)s() {
    local dir=""" + RUNTIME_DIR_SH + """ reply fd
    if zmodload zsh/net/socket 2>/dev/null && zsocket "$dir/complete.sock" 2>/dev/null; then
        fd=$REPLY
        print -rn -u $fd -- $'""" + PROTOCOL + """\\t\\tzsh\\t'"$EXPOSE_SYNQ_TOPIC"$'\\t\\t'"${words[1,$CURRENT]}"$'\\n'
        read -r -d '' -t 2 -u $fd reply
        exec {fd}>&-
    else
        reply="$(env _TYPER_COMPLETE_ARGS="${words[1,$CURRENT]}" %(autocomplete_var)s=complete_zsh %(prog_name)s)"
    fi
    eval "$reply"
}

# Autoloaded from $fpath, this file is the body of _%(prog_name)s and runs on every Tab
# unless later Tabs go straight to the function it defines.
compdef %(complete_func)s %(prog_name)s
if [[ $zsh_eval_context[-1] == loadautofunc ]]; then
    %(complete_func)s "$@"
fi
"""


class BashComplete(typer_shells.BashComplete):
    """Takes the raw line up to the cursor, and answers relative to bash's last word.

    ``request`` is ``(line, cur)``: the server sets it, the fallback reads them from the environment.
    """
    source_template = BASH
    request: tuple[str, str] | None = None

    def get_request(self) -> tuple[str, str]:
        if self.request is None:
            start_server()
            return os.environ.get("COMP_LINE", ""), os.environ.get("COMP_CUR", "")
        return self.request

    def get_completion_args(self) -> tuple[list[str], str]:
        line, _ = self.get_request()
        words = split_arg_string(line)
        incomplete = "" if not line or line[-1].isspace() else words.pop()
        return words[1:], incomplete

    def complete(self) -> str:
        args, incomplete = self.get_completion_args()
        _, cur = self.get_request()
        # What bash split off before its word: "host/proj" for "host/proj:sr", whose word is "sr".
        split_off = incomplete[:len(incomplete) - len(cur)] if incomplete.endswith(cur) else ""
        values = [item.value for item in self.get_completions(args, incomplete)]
        return "\n".join(v.removeprefix(split_off) for v in values if v.startswith(split_off))


class ZshComplete(typer_shells.ZshComplete):
    """No space after a ``TOPIC:`` or a directory, and no local files for a remote path.

    ``request`` is the line: the server sets it, the fallback reads ``_TYPER_COMPLETE_ARGS``.
    """
    source_template = ZSH
    request: str | None = None

    def get_completion_args(self) -> tuple[list[str], str]:
        if self.request is None:
            start_server()
            line = os.environ.get("_TYPER_COMPLETE_ARGS", "")
        else:
            line = self.request
        words = split_arg_string(line)[1:]
        if words and not line.endswith(" "):
            return words[:-1], words[-1]
        return words, ""

    def complete(self) -> str:
        args, incomplete = self.get_completion_args()
        values = [item.value for item in self.get_completions(args, incomplete)]
        if not values and ":" in incomplete:
            return ""
        if not any(v.endswith((":", "/")) for v in values):
            return super().complete()
        # Shown without what's typed up to its last ':' or '/', as zsh shows files.
        hidden = incomplete[:max(incomplete.rfind("/"), incomplete.rfind(":")) + 1]
        more = [v.removeprefix(hidden) for v in values if v.endswith((":", "/"))]
        done = [v.removeprefix(hidden) for v in values if not v.endswith((":", "/"))]
        lines = [f"compadd -p {shlex.quote(hidden)} -S '' -- {shlex.join(more)}"]
        if done:
            lines.append(f"compadd -p {shlex.quote(hidden)} -- {shlex.join(done)}")
        return "; ".join(lines)


class FishComplete(typer_shells.FishComplete):
    """Typer's, plus starting the server, which keeps the topics cache fresh."""

    def complete(self) -> str:
        start_server()
        return super().complete()


class Shell(StrEnum):
    bash = "bash"
    zsh = "zsh"
    fish = "fish"


def register() -> None:
    """Typer's shell classes, with ours. Typer only registers its own with add_completion."""
    typer_shells.completion_init()
    add_completion_class(BashComplete, "bash")
    add_completion_class(ZshComplete, "zsh")
    add_completion_class(FishComplete, "fish")


def script(shell: str, prog_name: str = PROG) -> str:
    """The completion script to source in ``shell``."""
    register()
    complete_var = f"_{prog_name.replace('-', '_').upper()}_COMPLETE"
    return get_completion_class(shell)(None, {}, prog_name, complete_var).source()


def answer(cli, shell: str, line: str, cur: str = "") -> str:
    """What the script expects back for ``line``, for the server: ``COMPREPLY`` lines for bash, code to eval for zsh."""
    if shell == "bash":
        complete = BashComplete(cli, {}, PROG, COMPLETE_VAR)
        complete.request = (line, cur)
    elif shell == "zsh":
        complete = ZshComplete(cli, {}, PROG, COMPLETE_VAR)
        complete.request = line
    else:
        raise ValueError(f"no server completion for {shell!r}")
    return complete.complete()
