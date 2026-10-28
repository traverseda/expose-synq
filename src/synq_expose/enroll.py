"""Asking a server for access: the box's side of enrollment.

A box with no credential for a server asks for one over HTTPS, prints a code a
person checks against the server's approval page, and polls until the request
is decided. The credential that comes back is a zenoh config layer, so this
side knows nothing about the transport or the kind of credential.

Protocol, relative to ``<server>/expose/api``:

    POST /enroll       {"hostname", "public_key"}   → {"id", "code"}
    GET  /enroll/<id>                               → {"status": "pending", "poll_after"}
                                                    | {"status": "denied" | "expired"}
                                                    | {"status": "approved", "expires", "zenoh", "files"}

- ``public_key`` is the box's Ed25519 public key, PEM (SubjectPublicKeyInfo).
- ``code`` is :func:`fingerprint` of that key. Both sides compute it; the box
  refuses a server whose code differs from its own.
- ``POST`` is idempotent per (hostname, key): asking again returns the same id.
- ``poll_after`` is in seconds.
- ``zenoh`` is a config layer, merged like a config file. A value
  ``{"$file": "<name>"}`` in it stands for the path of a file in the box's
  state directory: one of ``files`` (name → text), or ``key.pem``, the box's
  private key.
- Errors are a status code with ``{"error": "<message>"}``: 404 unknown id,
  409 hostname enrolled to another key, 422 invalid hostname or key, 429 too
  many requests (with ``Retry-After``).
"""
import base64
import hashlib
import importlib.metadata
import json
import os
import re
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey

API = "/expose/api/enroll"

# One key-expression chunk, and a hostname: no / * $ # ? @, never empty.
HOSTNAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,252}")
FILE_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,99}")
KEY_FILE, REQUEST_FILE, CREDENTIAL_FILE = "key.pem", "request.json", "credential.json"
RESERVED = {KEY_FILE, REQUEST_FILE, CREDENTIAL_FILE}

CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
POLL_DEFAULT, POLL_MIN, POLL_MAX = 3.0, 1.0, 60.0
HTTP_TIMEOUT = 30.0


def user_agent() -> str:
    try:
        version = importlib.metadata.version("expose-synq")
    except importlib.metadata.PackageNotFoundError:
        version = "unknown"
    return f"expose-synq/{version} (+https://github.com/traverseda/expose-synq)"


class EnrollError(RuntimeError):
    pass


def is_server(connect: str) -> bool:
    """A ``--connect`` value naming an enrollment server rather than a zenoh endpoint.

    zenoh endpoints are ``<protocol>/<address>``. A URL (``https://host``) or a
    bare host (``host``, ``host:port``) has no ``/`` after the scheme.
    """
    return "://" in connect or "/" not in connect


def server_url(connect: str) -> str:
    url = connect if "://" in connect else f"https://{connect}"
    if not urlsplit(url).netloc:
        raise EnrollError(f"{connect!r} isn't a server: give a host (hermes.example.com) or a URL")
    return url.rstrip("/")


def state_dir(url: str, config_dir: Path) -> Path:
    """Where the key, the pending request and the credential for ``url`` live."""
    return config_dir / "servers" / urlsplit(url).netloc


def fingerprint(public_key: Ed25519PublicKey) -> str:
    """The code a person compares: Crockford base32 of the first 40 bits of SHA-256 over the raw key, as XXXX-XXXX."""
    raw = public_key.public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
    bits = int.from_bytes(hashlib.sha256(raw).digest()[:5])
    chars = "".join(CROCKFORD[(bits >> shift) & 31] for shift in range(35, -1, -5))
    return f"{chars[:4]}-{chars[4:]}"


@dataclass
class Credential:
    hostname: str
    expires: datetime
    zenoh: dict[str, Any]
    """The zenoh config layer, ``$file`` references already replaced by paths."""

    @property
    def expired(self) -> bool:
        return self.expires <= datetime.now(timezone.utc)


def enroll(
    connect: str,
    hostname: str,
    config_dir: Path,
    *,
    on_code: Callable[[str, str], None] = lambda code, url: None,
    on_retry: Callable[[str], None] = lambda reason: None,
    sleep: Callable[[float], None] = time.sleep,
) -> Credential:
    """Return a credential for ``hostname`` on the server ``connect`` names, asking for one if there's none.

    A saved credential that hasn't expired is used without contacting the
    server. Otherwise this asks (or resumes asking, after a crash or Ctrl+C)
    and blocks until a person decides. ``on_code(code, url)`` is called once
    the request is in, so the code can be shown. Network errors and 5xx are
    retried at the poll interval, reported through ``on_retry``.
    """
    if not HOSTNAME.fullmatch(hostname):
        raise EnrollError(f"{hostname!r} can't be enrolled: a hostname is letters, digits, '.', '_' and '-'")
    url = server_url(connect)
    directory = state_dir(url, config_dir)
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)

    credential = load_credential(directory)
    if credential and credential.hostname == hostname and not credential.expired:
        return credential

    key = load_or_create_key(directory)
    code = fingerprint(key.public_key())
    request = read_json(directory / REQUEST_FILE)
    if not request or request.get("hostname") != hostname:
        request = ask(url, hostname, key, code, on_retry, sleep)
        write_private(directory / REQUEST_FILE, json.dumps(request))
        fresh = True
    else:
        fresh = False
    on_code(code, url)

    while True:
        status, body = call(url, "GET", f"{API}/{request['id']}", on_retry=on_retry, sleep=sleep)
        if status == 404:
            if fresh:
                raise EnrollError(f"{url} doesn't know the request it just gave us")
            # The server forgot a request we saved (lost its state, or the id expired): ask again.
            request = ask(url, hostname, key, code, on_retry, sleep)
            write_private(directory / REQUEST_FILE, json.dumps(request))
            fresh = True
            continue
        fresh = False
        if status != 200:
            raise EnrollError(f"{url}: {body.get('error') or f'HTTP {status}'}")
        state = body.get("status")
        if state == "pending":
            sleep(poll_interval(body.get("poll_after")))
        elif state in ("denied", "expired"):
            (directory / REQUEST_FILE).unlink(missing_ok=True)
            raise EnrollError(f"{url} {state} the request for {hostname}"
                              + (": run again to ask again" if state == "expired" else ""))
        elif state == "approved":
            credential = save_credential(directory, hostname, body)
            (directory / REQUEST_FILE).unlink(missing_ok=True)
            return credential
        else:
            raise EnrollError(f"{url} answered an unknown status {state!r}")


def ask(url, hostname, key, code, on_retry, sleep) -> dict:
    public_pem = key.public_key().public_bytes(
        serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo).decode()
    status, body = call(url, "POST", API, {"hostname": hostname, "public_key": public_pem},
                        on_retry=on_retry, sleep=sleep)
    if status not in (200, 201):
        raise EnrollError(f"{url} refused the request: {body.get('error') or f'HTTP {status}'}")
    if body.get("code") != code:
        raise EnrollError(f"{url} computed the code {body.get('code')!r} for our key, not {code}: "
                          f"something between us changed the key. Not enrolling.")
    if not isinstance(body.get("id"), str) or not body["id"] or "/" in body["id"]:
        raise EnrollError(f"{url} answered without a usable request id")
    return {"id": body["id"], "hostname": hostname}


def call(url: str, method: str, path: str, payload: dict | None = None, *,
         on_retry: Callable[[str], None], sleep: Callable[[float], None]) -> tuple[int, dict]:
    """One request, retried until the server gives an answer that isn't 429 or 5xx. Returns (status, JSON body)."""
    data = json.dumps(payload).encode() if payload is not None else None
    while True:
        req = urllib.request.Request(url + path, data=data, method=method,
                                     headers={"Content-Type": "application/json", "Accept": "application/json",
                                              "User-Agent": user_agent()})
        try:
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as response:
                return response.status, parse_body(response.read())
        except urllib.error.HTTPError as e:
            body = parse_body(e.read())
            if e.code == 429:
                on_retry(f"rate limited by {url}")
                sleep(poll_interval(e.headers.get("Retry-After")))
            elif e.code >= 500:
                on_retry(f"{url} answered HTTP {e.code}")
                sleep(POLL_DEFAULT)
            else:
                return e.code, body
        except (urllib.error.URLError, OSError) as e:
            on_retry(f"can't reach {url}: {getattr(e, 'reason', e)}")
            sleep(POLL_DEFAULT)


def parse_body(raw: bytes) -> dict:
    try:
        body = json.loads(raw or b"{}")
    except ValueError:
        return {}
    return body if isinstance(body, dict) else {}


def poll_interval(value) -> float:
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        return POLL_DEFAULT
    return min(max(seconds, POLL_MIN), POLL_MAX)


# ── State on disk ───────────────────────────────────────────────────────────


def write_private(path: Path, text: str) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(text)


def read_json(path: Path) -> dict | None:
    try:
        value = json.loads(path.read_text())
    except (OSError, ValueError):
        return None
    return value if isinstance(value, dict) else None


def load_or_create_key(directory: Path) -> Ed25519PrivateKey:
    path = directory / KEY_FILE
    if path.exists():
        key = serialization.load_pem_private_key(path.read_bytes(), password=None)
        if not isinstance(key, Ed25519PrivateKey):
            raise EnrollError(f"{path} isn't an Ed25519 key")
        return key
    key = Ed25519PrivateKey.generate()
    write_private(path, key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                          serialization.NoEncryption()).decode())
    return key


def load_credential(directory: Path) -> Credential | None:
    saved = read_json(directory / CREDENTIAL_FILE)
    if not saved:
        return None
    try:
        return Credential(saved["hostname"], parse_time(saved["expires"]), saved["zenoh"])
    except (KeyError, ValueError, TypeError):
        return None


def save_credential(directory: Path, hostname: str, body: dict) -> Credential:
    files, layer = body.get("files") or {}, body.get("zenoh")
    if not isinstance(files, dict) or not isinstance(layer, dict):
        raise EnrollError("the approval came without a zenoh config")
    for name, text in files.items():
        if not FILE_NAME.fullmatch(name) or name in RESERVED or not isinstance(text, str):
            raise EnrollError(f"the approval names a file we won't write: {name!r}")
    try:
        expires = parse_time(body["expires"])
    except (KeyError, ValueError, TypeError):
        raise EnrollError("the approval came without a valid expiry")
    resolved = resolve_files(layer, directory, set(files) | {KEY_FILE})
    for name, text in files.items():
        write_private(directory / name, text)
    # Written last: its presence means the files it points to are there.
    write_private(directory / CREDENTIAL_FILE,
                  json.dumps({"hostname": hostname, "expires": expires.isoformat(), "zenoh": resolved}))
    return Credential(hostname, expires, resolved)


def resolve_files(value: Any, directory: Path, known: set[str]) -> Any:
    """Replace each ``{"$file": name}`` with the path of that file in ``directory``."""
    if isinstance(value, dict):
        if set(value) == {"$file"}:
            name = value["$file"]
            if name not in known:
                raise EnrollError(f"the approval's zenoh config refers to a file it didn't send: {name!r}")
            return str(directory / name)
        return {k: resolve_files(v, directory, known) for k, v in value.items()}
    if isinstance(value, list):
        return [resolve_files(v, directory, known) for v in value]
    return value


def parse_time(value: str) -> datetime:
    when = datetime.fromisoformat(value)
    return when if when.tzinfo else when.replace(tzinfo=timezone.utc)
