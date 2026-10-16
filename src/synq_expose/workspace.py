"""The objects a remote agent talks to, and how they're laid out on the network.

    <topic>                 Workspace     always: what's exposed and what's granted
    <topic>/fs/read         FsRead        always
    <topic>/fs/write        FsWrite       --allow-write
    <topic>/shell           Shell         --allow-execute
    <root>/tickets/@<secret>              byte streams returned by open_read, open_write
                                          and spawn (tickets.py)

An object is only created for a granted capability, so an ungranted one has
nothing on the network to call. Each capability gets its own key prefix so
Zenoh ACL rules could target it later. The prototype doesn't use ACLs (see the
README's "Limitation: no access control"). Every remote method calls
``self._guard.check(...)`` first, for --confirm and the audit log.
"""
from dataclasses import dataclass

from SpiriSynq.syncable_objects import SyncableObject

from synq_expose.config import ExposeConfig
from synq_expose.guard import Guard
from synq_expose.policy import PathPolicy
from synq_expose.tickets import TicketRegistry


@dataclass
class Workspace(SyncableObject):
    root_name: str = ""
    can_write: bool = False
    can_execute: bool = False
    sandboxed: bool = True


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
    """Create the objects for the granted capabilities. The Workspace is first."""
    from synq_expose.fs import FsRead, FsWrite
    from synq_expose.shell import Shell

    rt = Runtime(config, PathPolicy.from_config(config), Guard.from_config(config), tickets)
    topic = config.topic

    objects = [
        start(
            Workspace, topic, rt,
            root_name=config.workspace.name,
            can_write=config.allow_write,
            can_execute=config.allow_execute,
            sandboxed=config.exec.sandbox.enabled,
        ),
        start(FsRead, f"{topic}/fs/read", rt),
    ]
    if config.allow_write:
        objects.append(start(FsWrite, f"{topic}/fs/write", rt))
    if config.allow_execute:
        objects.append(start(Shell, f"{topic}/shell", rt, max_procs=config.exec.max_procs))
    return objects
