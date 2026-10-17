"""Tickets: short-lived byte streams at ``<root>/tickets/<secret>``.

A ticket is a key prefix with a few raw zenoh queryables under it, not a
SpiriSynq object, so it has no ``sr_metadata`` or schema. ``<root>`` is the
node's base topic (e.g. ``azrael``), not the workspace topic. That keeps every
ticket on a node under one prefix.

A ticket's key is a bearer secret: anyone who learns it can use it. For now
the secret is an ordinary chunk, so a wildcard query or subscription on
``<root>/**`` reaches tickets too. Ticket routes ignore any query that doesn't
name them exactly, so a wildcard ``get`` can't close or kill anything, but a
wildcard subscription sees stream data. It used to be a verbatim ``@`` chunk, which
no wildcard matches, but no zenoh ACL rule can match one either, so a router
confining a node to ``<root>/**`` blocked every ticket. This prototype has no
other access control (see the README).

Every stream is query-shaped, not pub/sub. Each request gets a reply or
``reply_err``, and multi-reply queries carry data back. The route handlers
live with what they serve: fs.py and shell.py.

Handlers run on a worker pool, not zenoh's callback thread, so one that
blocks (a long poll, a large read) doesn't hold up anything else. A query
outlives its callback until it's dropped, so the worker replies and then
drops it, which finishes the query for the client.

This is a local stand-in for SpiriSynq native ticketing (#205).
"""
import json
import secrets
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

import zenoh
from loguru import logger

DEFAULT_TTL = 30.0
"""Seconds a ticket may go without a request before it's revoked. ``<t>/heartbeat`` counts."""

RETIRE_GRACE = 30.0
"""Seconds a retired ticket keeps answering repeats (see ``Ticket.retire``)."""

DEFAULT_WORKERS = 32
"""Requests handled at once, across all tickets. A long poll holds a worker
for its whole wait; past this, requests queue."""

Handler = Callable[[zenoh.Query], None]




@dataclass
class Ticket:
    key: str
    kind: str
    on_revoke: Callable[[], None] | None = None
    last_used: float = field(default_factory=time.monotonic)
    done: bool = False
    """Set by a handler when the ticket is finished (commit, abort, process
    reaped). Later requests are refused, and the registry revokes it on its next
    sweep. Handlers never undeclare their own queryable from inside its callback."""
    retire_at: float | None = None
    """Set by ``retire``: the ticket keeps serving until then, and the next sweep after revokes it."""
    queryables: list = field(default_factory=list)

    def retire(self, grace: float = RETIRE_GRACE) -> None:
        """Finished, but keep answering for ``grace`` seconds so a client that
        lost a reply can repeat its request and get the same answer."""
        self.retire_at = time.monotonic() + grace


class TicketRegistry:
    def __init__(self, zenoh_session: zenoh.Session, root: str, ttl: float = DEFAULT_TTL,
                 workers: int = DEFAULT_WORKERS):
        self._session = zenoh_session
        self.prefix = f"{root}/tickets"
        self.ttl = ttl
        self._lock = threading.Lock()
        self._tickets: dict[str, Ticket] = {}
        self._pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="ticket")

    @property
    def session(self) -> zenoh.Session:
        """For handlers that publish streams under their ticket."""
        return self._session

    def mint(self, kind: str, routes: dict[str, Handler], on_revoke: Callable[[], None] | None = None) -> Ticket:
        """Create a ticket and serve ``routes`` under it. A route named ``""`` serves the ticket key itself.

        Every ticket also gets ``<t>/heartbeat``, which counts as use and
        replies ``{"ttl": seconds}``. Only requests count: work the ticket does on its own, like
        publishing a stream, doesn't keep it alive. A client holding a
        ticket it isn't otherwise using heartbeats more often than ``ttl``.
        """
        if "heartbeat" in routes:
            raise ValueError("'heartbeat' is served by the registry")
        ticket = Ticket(f"{self.prefix}/{secrets.token_hex(16)}", kind, on_revoke)
        routes = {**routes, "heartbeat": self._heartbeat}
        for name, handler in routes.items():
            key = f"{ticket.key}/{name}" if name else ticket.key
            ticket.queryables.append(self._session.declare_queryable(key, self._dispatch(ticket, key, handler)))
        with self._lock:
            self._tickets[ticket.key] = ticket
        logger.debug(f"Minted {kind} ticket {self.prefix}/{ticket.key.rsplit('/', 1)[1][:4]}…")
        return ticket

    def _heartbeat(self, query: zenoh.Query) -> None:
        query.reply(query.key_expr, json.dumps({"ttl": self.ttl}), encoding=zenoh.Encoding.APPLICATION_JSON)

    def _dispatch(self, ticket: Ticket, key: str, handler: Handler):
        def run(query: zenoh.Query):
            try:
                if ticket.done:
                    query.reply_err("ticket is closed")
                    return
                ticket.last_used = time.monotonic()
                handler(query)
            except Exception as e:
                logger.exception(f"Error in {ticket.kind} ticket handler")
                query.reply_err(f"ticket error: {type(e).__name__}: {e}")
            finally:
                ticket.last_used = time.monotonic()
                query.drop()

        def callback(query: zenoh.Query):
            # A wildcard query (`get <root>/**`) reaches every ticket's routes,
            # close and kill included. Only a query naming this route is for it.
            if str(query.key_expr) != key:
                query.drop()
                return
            try:
                self._pool.submit(run, query)
            except RuntimeError:  # pool shut down: the registry is closing
                query.reply_err("ticket is closed")
                query.drop()
        return callback

    def revoke(self, key: str) -> None:
        with self._lock:
            ticket = self._tickets.pop(key, None)
        if ticket is None:
            return
        ticket.done = True
        for queryable in ticket.queryables:
            queryable.undeclare()
        if ticket.on_revoke is not None:
            try:
                ticket.on_revoke()
            except Exception:
                logger.exception(f"Error cleaning up {ticket.kind} ticket")

    def sweep(self) -> None:
        """Revoke finished and idle tickets. Called periodically from the main loop."""
        now = time.monotonic()
        with self._lock:
            stale = [
                t.key for t in self._tickets.values()
                if t.done or (t.retire_at is not None and now > t.retire_at) or now - t.last_used > self.ttl
            ]
        for key in stale:
            self.revoke(key)

    def close(self) -> None:
        with self._lock:
            keys = list(self._tickets)
        for key in keys:
            self.revoke(key)
        # Running handlers finish on their own; queued ones are dropped.
        self._pool.shutdown(wait=False, cancel_futures=True)

    def __len__(self) -> int:
        return len(self._tickets)
