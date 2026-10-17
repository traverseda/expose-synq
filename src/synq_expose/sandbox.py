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


SYSTEM_RO = ("/usr", "/bin", "/sbin", "/lib", "/lib32", "/lib64", "/nix", "/run/current-system")
ETC_RO = ("passwd", "group", "hosts", "hostname", "nsswitch.conf", "resolv.conf", "localtime",
          "os-release", "ssl", "pki", "static", "profiles")
"""The parts of /etc a command needs to resolve users, hosts and TLS. On NixOS most are
symlinks into /etc/static; a bind mount follows them, and /etc/static is bound too."""


def wrap_command(
    argv: list[str],
    *,
    workspace: Path,
    writable: bool,
    hidden: list[Path],
    config: ExecConfig,
    pty: bool = False,
    home: str | None = None,
) -> list[str]:
    """Return the full argv to spawn for ``argv``.

    Unsandboxed, that is ``argv`` itself. Sandboxed, it is ``bwrap ... -- argv`` with:

    * the workspace bound at its own path (rw if ``writable`` else ro), cwd set to it
    * ``/usr``, ``/bin``, ``/lib*``, minimal ``/etc``, and on NixOS ``/nix``
      and ``/run/current-system`` bound read-only
    * private ``/tmp``, ``/proc``, ``/dev``, and an empty ``home`` so tools can
      write their caches somewhere
    * ``--unshare-all``, plus ``--share-net`` unless ``config.sandbox.allow_network`` is off
    * ``--die-with-parent``, ``--new-session``; the environment is whatever
      the caller spawns bwrap with (already just PATH, HOME, LANG and ``pass_env``)
    * each path in ``hidden`` masked with an empty tmpfs (dirs) or /dev/null (files)
    * ``config.sandbox.bind_ro`` / ``bind`` added as extra mounts

    On a pty, ``--new-session`` would detach the command from its terminal
    (no Ctrl-C, no SIGWINCH). The pty is the command's own, so the TIOCSTI
    escape it guards against has nowhere to go: leave it out there.
    """
    sandbox = config.sandbox
    if not sandbox.enabled:
        return argv
    args = [find_bwrap(), "--unshare-all"]
    if sandbox.allow_network:
        args.append("--share-net")
    args += ["--die-with-parent", "--proc", "/proc", "--dev", "/dev", "--tmpfs", "/tmp"]
    if not pty:
        args.append("--new-session")
    for path in SYSTEM_RO:
        args += ["--ro-bind-try", path, path]
    for name in ETC_RO:
        args += ["--ro-bind-try", f"/etc/{name}", f"/etc/{name}"]
    if home:
        args += ["--dir", home]
    ws = str(workspace)
    args += ["--bind" if writable else "--ro-bind", ws, ws]
    for path in hidden:
        if path.is_symlink():
            continue  # a mount would land on its target; the link only reaches what's already visible or not mounted
        if path.is_dir():
            args += ["--tmpfs", str(path)]
        else:
            args += ["--ro-bind", "/dev/null", str(path)]
    for path in sandbox.bind_ro:
        args += ["--ro-bind", str(path), str(path)]
    for path in sandbox.bind:
        args += ["--bind", str(path), str(path)]
    return [*args, "--chdir", ws, "--", *argv]
