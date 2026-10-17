import time

import pytest
import zenoh

from synq_expose.config import build_zenoh_config
from synq_expose.tickets import TicketRegistry


@pytest.fixture
def session():
    # Locked down: no network, the session only answers its own queries.
    s = zenoh.open(build_zenoh_config(environ={}))
    yield s
    s.close()


def replies(session, selector):
    return [
        r.ok.payload.to_string() if r.ok else f"ERR {r.err.payload.to_string()}"
        for r in session.get(selector, timeout=1)
    ]


def echo(query):
    query.reply(query.key_expr, payload=f"hello {query.parameters.get('name')}")


def test_ticket_routes_answer_under_its_key(session):
    registry = TicketRegistry(session, "node")
    ticket = registry.mint("test", {"": echo, "sub": echo})
    assert ticket.key.startswith("node/tickets/")
    assert replies(session, f"{ticket.key}?name=a") == ["hello a"]
    assert replies(session, f"{ticket.key}/sub?name=b") == ["hello b"]


def test_wildcard_queries_reach_no_route(session):
    registry = TicketRegistry(session, "node")
    closed = []
    ticket = registry.mint("test", {"": echo, "close": lambda q: closed.append(q) or q.reply(q.key_expr, "{}")})
    for selector in ("node/**", "node/tickets/**", f"{ticket.key}/*", "**"):
        assert replies(session, selector) == []
    assert closed == []
    assert replies(session, f"{ticket.key}/close") == ["{}"]
    assert len(closed) == 1


def test_revoke_and_done(session):
    cleaned = []
    registry = TicketRegistry(session, "node")
    ticket = registry.mint("test", {"": echo}, on_revoke=lambda: cleaned.append(True))

    ticket.done = True
    assert replies(session, ticket.key) == ["ERR ticket is closed"]

    registry.sweep()
    assert len(registry) == 0
    assert cleaned == [True]
    assert replies(session, ticket.key) == []


def test_heartbeat_counts_as_use(session):
    registry = TicketRegistry(session, "node", ttl=0.2)
    ticket = registry.mint("test", {"": echo})
    for _ in range(4):
        time.sleep(0.1)
        assert replies(session, f"{ticket.key}/heartbeat") == ['{"ttl": 0.2}']
        registry.sweep()
    assert replies(session, f"{ticket.key}?name=a") == ["hello a"]


def test_heartbeat_route_is_reserved(session):
    with pytest.raises(ValueError, match="heartbeat"):
        TicketRegistry(session, "node").mint("test", {"heartbeat": echo})


def test_idle_tickets_expire(session):
    registry = TicketRegistry(session, "node", ttl=0.2)
    ticket = registry.mint("test", {"": echo})
    time.sleep(0.3)
    registry.sweep()
    assert replies(session, ticket.key) == []


def test_handler_errors_become_reply_err(session):
    def boom(query):
        raise ValueError("nope")

    registry = TicketRegistry(session, "node")
    ticket = registry.mint("test", {"": boom})
    assert replies(session, ticket.key) == ["ERR ticket error: ValueError: nope"]


def test_slow_handler_does_not_block_others(session):
    registry = TicketRegistry(session, "node")
    slow = registry.mint("test", {"": lambda q: (time.sleep(1), echo(q))})
    fast = registry.mint("test", {"": echo})
    pending = session.get(f"{slow.key}?name=slow", timeout=5)
    start = time.monotonic()
    assert replies(session, f"{fast.key}?name=fast") == ["hello fast"]
    assert time.monotonic() - start < 0.5
    assert [r.ok.payload.to_string() for r in pending] == ["hello slow"]
    registry.close()
