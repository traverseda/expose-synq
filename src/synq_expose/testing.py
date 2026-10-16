"""A two-node loopback network for tests and benchmarks.

The shared workspace runs on one SpiriSynq session and the agent on another.
Both are locked down (no scouting) and joined over TCP on 127.0.0.1, so the
real wire path is exercised and nothing reaches the outside network.

Import this before creating any SpiriSynq object: it locks down the default
session that SpiriSynq opens at import (#243).
"""
import socket
import time
from dataclasses import dataclass, field
from pathlib import Path

import zenoh
from SpiriSynq.session import Session, current_session

from synq_expose.config import ExposeConfig, build_zenoh_config, reopen_session
from synq_expose.tickets import TicketRegistry

if not len(current_session.get().objects):
    reopen_session(current_session.get(), build_zenoh_config(environ={}))

SERVER_ROOT = "node"


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_for(z: zenoh.Session, selector: str, timeout: float = 5.0) -> None:
    """Block until something answers ``selector``. Remote declarations take a moment to arrive."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if list(z.get(selector, timeout=0.5)):
            return
        time.sleep(0.02)
    raise TimeoutError(f"nothing answered {selector}")


@dataclass
class Network:
    server: Session
    client: Session
    endpoint: str
    """Where the server listens, for other processes to connect to."""

    @classmethod
    def open(cls) -> "Network":
        endpoint = f"tcp/127.0.0.1:{free_port()}"
        server = Session(config=build_zenoh_config(listen=[endpoint], environ={}), base_topic=SERVER_ROOT)
        client = Session(config=build_zenoh_config(connect=[endpoint], environ={}), base_topic="client")
        return cls(server, client, endpoint)

    @property
    def z(self) -> zenoh.Session:
        """The client's raw zenoh session, for ticket streams."""
        return self.client.zenoh_session

    def share(self, config: ExposeConfig, ttl: float | None = None) -> "Share":
        """Serve ``config`` on the server side and wait until the client can see every object."""
        from synq_expose.workspace import build_objects

        tickets = TicketRegistry(self.server.zenoh_session, SERVER_ROOT, **({} if ttl is None else {"ttl": ttl}))
        with self.server.as_default():
            objects = build_objects(config, tickets)
        for obj in objects:
            wait_for(self.z, f"{obj.synq_absolute_path}/sr_metadata")
        return Share(self, config, tickets, objects)

    def close(self) -> None:
        self.client.close()
        self.server.close()


@dataclass
class Share:
    """One running ``expose-synq share``, as seen from both ends."""
    network: Network
    config: ExposeConfig
    tickets: TicketRegistry
    objects: list
    mirrors: list = field(default_factory=list)

    @property
    def topic(self) -> str:
        return self.objects[0].synq_absolute_path

    @property
    def workspace(self) -> Path:
        return self.config.workspace

    @property
    def z(self) -> zenoh.Session:
        return self.network.z

    def mirror(self, cls, suffix: str = ""):
        """A client-side mirror of the object at ``<topic><suffix>``, e.g. ``mirror(FsRead, "/fs/read")``."""
        with self.network.client.as_default():
            mirror = cls.from_topic(f"{self.topic}{suffix}")
        self.mirrors.append(mirror)
        return mirror

    def exists(self, suffix: str) -> bool:
        return bool(list(self.z.get(f"{self.topic}{suffix}/sr_metadata", timeout=0.5)))

    def close(self) -> None:
        for mirror in self.mirrors:
            mirror.close()
        self.tickets.close()
        for obj in reversed(self.objects):
            obj.close()
