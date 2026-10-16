"""Process execution, modelled on traverseda/zenoh-fs's pull-based sessions.

``Shell.spawn`` starts a process and returns a ticket (tickets.py) for it.
Output is buffered on the server, and clients pull it by byte offset. That way
a long build isn't tied to one query's timeout, and a client that drops can
reconnect and carry on from its last offset.

For a shell that keeps state between commands, spawn ``bash`` with stdin open
and send it commands on ``/stdin``.

Anything allowed to run here can read whatever the sandbox can see.
"""
from dataclasses import dataclass

import zenoh
from SpiriSynq.remote_callables import remote_method
from SpiriSynq.syncable_objects import SyncableObject


@dataclass
class Shell(SyncableObject):
    max_procs: int = 4
    running: int = 0

    @remote_method()
    def spawn(
        self,
        command: list[str] | str,
        stdin: bool = False,
        timeout: float | None = None,
    ) -> str:
        """Start ``command`` with cwd at the workspace root, sandboxed unless --no-sandbox.

        A ``str`` goes through the configured shell. ``stdin=True`` keeps stdin
        open for ``/stdin``, otherwise it's /dev/null. Returns the ticket key.
        See ``ProcessStreams``.
        """
        self._guard.check("spawn", command=command)
        raise NotImplementedError


class ProcessStreams:
    """Process ticket. Routes under the ticket key::

        get <t>/stdout?offset=N[;wait=S]   → raw bytes from N on (empty if none yet). With wait,
                                             blocks up to S seconds for output past N (long poll).
        get <t>/stderr?offset=N[;wait=S]   → same, for stderr
        get <t>/stdin  + payload           → written to stdin. Empty payload closes it (EOF)
        get <t>/status                     → {running, exit_code, stdout_size, stderr_size}
        get <t>/kill?signal=SIGTERM        → signal the process
        get <t>/discard                    → kill if running, drop the output, close the ticket

    Output stays readable after the process exits, until the ticket is discarded or idles out.
    TODO: cap how much output is kept.
    """

    def __init__(self, argv: list[str], stdin: bool):
        self.argv = argv
        self.stdin = stdin

    def routes(self) -> dict:
        return {
            "stdout": self.stdout,
            "stderr": self.stderr,
            "stdin": self.write_stdin,
            "status": self.status,
            "kill": self.kill,
            "discard": self.discard,
        }

    def stdout(self, query: zenoh.Query) -> None:
        raise NotImplementedError

    def stderr(self, query: zenoh.Query) -> None:
        raise NotImplementedError

    def write_stdin(self, query: zenoh.Query) -> None:
        raise NotImplementedError

    def status(self, query: zenoh.Query) -> None:
        raise NotImplementedError

    def kill(self, query: zenoh.Query) -> None:
        raise NotImplementedError

    def discard(self, query: zenoh.Query) -> None:
        raise NotImplementedError
