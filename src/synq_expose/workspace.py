"""The objects a remote agent talks to, and how they're laid out on the network.

    <topic>/fs/read         FsRead        always
    <topic>/fs/write        FsWrite       --allow-write
    <topic>/shell           Shell         --allow-execute
    <root>/tickets/@<secret>              byte streams returned by open_read, open_write
                                          and spawn (tickets.py)

An object is only created for a granted capability, so an ungranted one has
nothing on the network to call, and what a share grants is found with
SpiriSynq's own discovery (``discover``). Every remote method calls
``self._guard.check(...)`` first, for --confirm and the audit log.
"""
import time
from dataclasses import dataclass

from SpiriSynq.syncable_objects import SyncableObject

from synq_expose.config import ExposeConfig
from synq_expose.guard import Guard
from synq_expose.policy import PathPolicy
from synq_expose.tickets import TicketRegistry

FS_READ = "fs/read"
"""Where FsRead sits under a share's topic. It's always there, so it's what marks a share."""


@dataclass
class Runtime:
    """What every object's methods need, attached as plain (never synced) attributes."""
    config: ExposeConfig
    policy: PathPolicy
    guard: Guard
    tickets: TicketRegistry


def start(cls: type[SyncableObject], topic: str, rt: Runtime, **fields):
    """Create an authoritative object with the runtime attached, then start syncing it."""
    obj = cls(topic, synq_authoritive=True, synq_auto_start=False, **fields)
    obj._config = rt.config
    obj._policy = rt.policy
    obj._guard = rt.guard
    obj._tickets = rt.tickets
    obj.sync()
    return obj


def build_objects(config: ExposeConfig, tickets: TicketRegistry) -> list[SyncableObject]:
    """Create the objects for the granted capabilities. FsRead is first."""
    from synq_expose.fs import FsRead, FsWrite
    from synq_expose.shell import Shell

    rt = Runtime(config, PathPolicy.from_config(config), Guard.from_config(config), tickets)
    topic = config.topic

    objects = [start(FsRead, f"{topic}/{FS_READ}", rt)]
    if config.allow_write:
        objects.append(start(FsWrite, f"{topic}/fs/write", rt))
    if config.allow_execute:
        objects.append(start(Shell, f"{topic}/shell", rt, max_procs=config.exec.max_procs,
                             sandboxed=config.exec.sandbox.enabled))
    return objects


def share_topic(objects: list[SyncableObject]) -> str:
    """The absolute topic of the share ``build_objects`` made."""
    return objects[0].synq_absolute_path.removesuffix(f"/{FS_READ}")


class DiscoveryError(LookupError):
    pass


def discover(session, topic: str, timeout: float = 10.0) -> dict[str, str]:
    """What the share at ``topic`` grants: ``{class name: object topic}``, e.g. ``{"FsRead": ..., "FsWrite": ...}``.

    SpiriSynq's discovery (``list_topics``), retried until the share's
    objects show up or ``timeout`` passes, since a fresh session takes a
    moment to learn remote declarations. One answer covers every object
    the share has. Raises DiscoveryError if there's no share at ``topic``,
    or more than one under it.
    """
    deadline = time.monotonic() + timeout
    while True:
        found: dict[str, list[str]] = {}
        for meta in session.list_topics(prefix=topic):
            for cls in meta["classes"]:
                found.setdefault(cls.removeprefix("!"), []).append(meta["topic"])
        shares = found.get("FsRead", [])
        if len(shares) > 1:
            listed = ", ".join(sorted(t.removesuffix(f"/{FS_READ}") for t in shares))
            raise DiscoveryError(f"{topic} holds more than one share: {listed}")
        if shares:
            if shares[0] != f"{topic}/{FS_READ}":
                raise DiscoveryError(f"no share at {topic}; there's one at {shares[0].removesuffix(f'/{FS_READ}')}")
            return {cls: topics[0] for cls, topics in found.items()}
        if time.monotonic() >= deadline:
            raise DiscoveryError(f"no share at {topic}; check the topic and --connect")
        time.sleep(0.05)
