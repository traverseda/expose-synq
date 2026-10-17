"""Building the argv that runs a remote command, inside bubblewrap or not."""
import shutil
from pathlib import Path

from synq_expose.config import ExecConfig


class SandboxUnavailable(RuntimeError):
    pass


def find_bwrap() -> str:
    path = shutil.which("bwrap")
    if path is None:
        raise SandboxUnavailable("bubblewrap (bwrap) not found on PATH; install it or pass --no-sandbox")
    return path


def wrap_command(
    argv: list[str],
    *,
    workspace: Path,
    writable: bool,
    hidden: list[Path],
    config: ExecConfig,
) -> list[str]:
    """Return the full argv to spawn for ``argv``.

    Unsandboxed, that is ``argv`` itself. Sandboxed, it is ``bwrap ... -- argv`` with:

    * the workspace bound at its own path (rw if ``writable`` else ro), cwd set to it
    * ``/usr``, ``/bin``, ``/lib*``, minimal ``/etc``, and on NixOS ``/nix/store``
      and ``/run/current-system`` bound read-only
    * private ``/tmp``, ``/proc``, ``/dev``
    * ``--unshare-all``, plus ``--share-net`` unless ``config.sandbox.allow_network`` is off
    * ``--die-with-parent``, ``--new-session``, ``--clearenv`` plus ``config.pass_env``
    * each path in ``hidden`` masked with an empty tmpfs (dirs) or /dev/null (files)
    * ``config.sandbox.bind_ro`` / ``bind`` added as extra mounts

    On a pty, ``--new-session`` would detach the command from its terminal
    (no Ctrl-C, no SIGWINCH). The pty is the command's own, so the TIOCSTI
    escape it guards against has nowhere to go: leave it out there.
    """
    if not config.sandbox.enabled:
        return argv
    raise NotImplementedError("bubblewrap argv construction")
