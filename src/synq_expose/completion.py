"""Tab completion of share topics, from a cache file that's refreshed in the background.

A Tab press can't wait on the network: a fresh zenoh session takes a while
to learn what's out there, so asking it live would take a second or more
and could still come back empty. Completion reads the topics found last
time from ``~/.cache/expose-synq/topics`` instead, and when that's older
than ``MAX_AGE`` starts ``expose-synq _refresh-topics`` detached to find
them again, for the next Tab.

Typer does the rest: the shell runs ``expose-synq`` with
``_EXPOSE_SYNQ_COMPLETE`` set, Click parses the line, and the parameter's
``autocompletion`` callback answers. Two of Typer's shell classes are
replaced, both for ``TOPIC:PATH``:

- bash splits words at ``:``, so ``host/proj:sr`` reaches the completion
  function as ``host/proj`` ``:`` ``sr``. Ours hands Click the raw line
  instead and trims each answer back to bash's last word, as scp's
  completion does.
- zsh adds a space after a single match. A candidate ending in ``:`` is a
  topic waiting for its path, so it gets none (bash: ``compopt -o nospace``).
"""
import fcntl
import os
import shlex
import subprocess
import sys
import time
from collections.abc import Mapping
from enum import StrEnum
from pathlib import Path

# Typer's shell classes and its vendored Click: private, but what --install-completion itself uses.
from typer import _completion_classes as typer_shells
from typer._click.shell_completion import add_completion_class, get_completion_class, split_arg_string

MAX_AGE = 30.0
"""Seconds before the cached topics are looked up again."""

REFRESH_WINDOW = 1.5
"""Seconds a refresh listens for shares. A new session needs a moment to learn what's out there."""

REFRESH_GAP = 5.0
"""Seconds after starting a refresh before another may start, so a burst of Tabs starts one."""

SUFFIX = "/fs/read/sr_metadata/FsRead"
"""What follows a share's topic in the key of its FsRead metadata (workspace.FS_READ, then SpiriSynq's)."""


def cache_path(environ: Mapping[str, str] = os.environ) -> Path:
    base = environ.get("XDG_CACHE_HOME") or Path.home() / ".cache"
    return Path(base) / "expose-synq" / "topics"


def topics(path: Path | None = None) -> list[str]:
    """The cached topics, starting a refresh if they're stale. Never waits on the network."""
    path = path or cache_path()
    try:
        age = time.time() - path.stat().st_mtime
        found = path.read_text().splitlines()
    except FileNotFoundError:
        age, found = float("inf"), []
    if age > MAX_AGE:
        refresh_in_background(path)
    return found


def refresh_in_background(path: Path) -> None:
    lock = path.with_name(f"{path.name}.lock")
    try:
        if time.time() - lock.stat().st_mtime < REFRESH_GAP:
            return
    except FileNotFoundError:
        lock.parent.mkdir(parents=True, exist_ok=True)
    lock.touch()
    subprocess.Popen(
        [sys.executable, "-m", "synq_expose", "_refresh-topics", "--timeout", str(REFRESH_WINDOW)],
        stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        cwd="/", start_new_session=True,
    )


def complete_topic(incomplete: str) -> list[str]:
    """Autocompletion for a bare TOPIC."""
    return [t for t in topics() if t.startswith(incomplete)]


def complete_remote(incomplete: str) -> list[str]:
    """Autocompletion for [TOPIC:]PATH: the TOPIC: part. Paths would need the share itself, so none.

    A topic holding a ``:`` is left out, since ``TOPIC:PATH`` splits at the first one.
    """
    if ":" in incomplete or incomplete.startswith("./"):
        return []
    return [f"{t}:" for t in topics() if ":" not in t and t.startswith(incomplete)]


def refresh(zenoh_config, path: Path, window: float = REFRESH_WINDOW) -> bool:
    """Look up the shares on the network and write their topics to ``path``, one per line.

    Returns False without looking if another refresh holds the lock.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path.with_name(f"{path.name}.lock"), "a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return False
        found = scan(zenoh_config, window)
        tmp = path.with_name(f".{path.name}.{os.getpid()}")
        tmp.write_text("".join(f"{topic}\n" for topic in sorted(found)))
        os.replace(tmp, path)
    return True


def scan(zenoh_config, window: float) -> set[str]:
    """Every share's topic that answers within ``window`` seconds, asking again as the session learns routes.

    Plain zenoh rather than SpiriSynq, whose import alone opens a session:
    the reply's key is all that's needed.
    """
    import zenoh
    found: set[str] = set()
    deadline = time.monotonic() + window
    z = zenoh.open(zenoh_config)
    try:
        while (remaining := deadline - time.monotonic()) > 0:
            for reply in z.get(f"**{SUFFIX}", consolidation=zenoh.ConsolidationMode.NONE, timeout=remaining):
                if reply.ok:
                    found.add(str(reply.ok.key_expr).removesuffix(SUFFIX))
            time.sleep(0.1)
    finally:
        z.close()
    return found


# ── Shells ──────────────────────────────────────────────────────────────

BASH = """\
%(complete_func)s() {
    local IFS=$'\\n' c
    COMPREPLY=( $(env COMP_LINE="${COMP_LINE:0:COMP_POINT}" COMP_CUR="${COMP_WORDS[COMP_CWORD]}" \\
                      %(autocomplete_var)s=complete_bash "$1") )
    if [[ ${#COMPREPLY[@]} == 1 && ${COMPREPLY[0]} == *: ]]; then
        compopt -o nospace
    fi
    return 0
}

complete -o default -F %(complete_func)s %(prog_name)s
"""

ZSH = """\
#compdef %(prog_name)s

%(complete_func)s() {
    eval "$(env _TYPER_COMPLETE_ARGS="${words[1,$CURRENT]}" %(autocomplete_var)s=complete_zsh %(prog_name)s)"
}

if [[ $zsh_eval_context[-1] == loadautofunc ]]; then
    %(complete_func)s "$@"
else
    compdef %(complete_func)s %(prog_name)s
fi
"""


class BashComplete(typer_shells.BashComplete):
    """Takes the raw line up to the cursor, and answers relative to bash's last word."""
    source_template = BASH

    def get_completion_args(self) -> tuple[list[str], str]:
        line = os.environ.get("COMP_LINE", "")
        words = split_arg_string(line)
        incomplete = "" if not line or line[-1].isspace() else words.pop()
        return words[1:], incomplete

    def complete(self) -> str:
        args, incomplete = self.get_completion_args()
        cur = os.environ.get("COMP_CUR", "")
        # What bash split off before its word: "host/proj" for "host/proj:sr", whose word is "sr".
        split_off = incomplete[:len(incomplete) - len(cur)] if incomplete.endswith(cur) else ""
        values = [item.value for item in self.get_completions(args, incomplete)]
        return "\n".join(v.removeprefix(split_off) for v in values if v.startswith(split_off))


class ZshComplete(typer_shells.ZshComplete):
    """No space after a ``TOPIC:``."""
    source_template = ZSH

    def complete(self) -> str:
        args, incomplete = self.get_completion_args()
        values = [item.value for item in self.get_completions(args, incomplete)]
        if values and all(v.endswith(":") for v in values):
            return f"compadd -S '' -- {shlex.join(values)}"
        return super().complete()


class Shell(StrEnum):
    bash = "bash"
    zsh = "zsh"
    fish = "fish"


def register() -> None:
    """Typer's shell classes, with ours for bash and zsh. Typer only registers its own with add_completion."""
    typer_shells.completion_init()
    add_completion_class(BashComplete, "bash")
    add_completion_class(ZshComplete, "zsh")


def script(shell: str, prog_name: str) -> str:
    """The completion script to source in ``shell``."""
    complete_var = f"_{prog_name.replace('-', '_').upper()}_COMPLETE"
    return get_completion_class(shell)(None, {}, prog_name, complete_var).source()
