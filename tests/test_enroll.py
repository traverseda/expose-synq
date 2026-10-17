import json
import stat
import threading
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from synq_expose.enroll import API, EnrollError, enroll, fingerprint, is_server, server_url, state_dir


class FakeServer:
    """The server side of the protocol in synq_expose.enroll, scripted by the test.

    ``decisions`` is what successive polls answer; the last one repeats.
    """

    def __init__(self):
        self.decisions = [{"status": "pending", "poll_after": 0}]
        self.post_status = None          # e.g. (409, "taken"), to refuse the POST
        self.code_override = None
        self.fail_next = 0               # answer this many requests with 503 first
        self.posts, self.gets = [], []
        self.forget = False              # answer GETs 404 until the next POST
        server = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def reply(self, status, body):
                data = json.dumps(body).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def failing(self):
                if server.fail_next:
                    server.fail_next -= 1
                    self.reply(503, {"error": "restarting"})
                    return True
                return False

            def do_POST(self):
                if self.failing():
                    return
                assert self.path == API
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                server.posts.append(body)
                server.forget = False
                if server.post_status:
                    status, error = server.post_status
                    return self.reply(status, {"error": error})
                key = serialization.load_pem_public_key(body["public_key"].encode())
                self.reply(201, {"id": f"req{len(server.posts)}", "code": server.code_override or fingerprint(key)})

            def do_GET(self):
                if self.failing():
                    return
                request_id = self.path.removeprefix(API + "/")
                server.gets.append(request_id)
                if server.forget or not request_id.startswith("req"):
                    return self.reply(404, {"error": "unknown request"})
                decision = server.decisions.pop(0) if len(server.decisions) > 1 else server.decisions[0]
                self.reply(200, decision)

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.httpd.server_port}"
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()


def approval(expires=None, **extra):
    expires = expires or datetime.now(timezone.utc) + timedelta(hours=24)
    return {
        "status": "approved",
        "expires": expires.isoformat(),
        "zenoh": {
            "connect": {"endpoints": ["tls/hermes.example:7447"]},
            "transport": {"link": {"tls": {
                "root_ca_certificate": {"$file": "ca.pem"},
                "connect_certificate": {"$file": "cert.pem"},
                "connect_private_key": {"$file": "key.pem"},
            }}},
        },
        "files": {"ca.pem": "CA\n", "cert.pem": "CERT\n"},
        **extra,
    }


@pytest.fixture
def server():
    s = FakeServer()
    yield s
    s.close()


def run(server, tmp_path, hostname="boxa", **kwargs):
    codes = []
    credential = enroll(server.url, hostname, tmp_path, on_code=lambda code, url: codes.append(code),
                        sleep=lambda seconds: None, **kwargs)
    return credential, codes


def test_fingerprint_is_crockford_base32_of_the_key_hash():
    key = Ed25519PrivateKey.from_private_bytes(bytes(32)).public_key()
    code = fingerprint(key)
    assert code == fingerprint(key)
    assert len(code) == 9 and code[4] == "-"
    assert set(code.replace("-", "")) <= set("0123456789ABCDEFGHJKMNPQRSTVWXYZ")
    other = Ed25519PrivateKey.from_private_bytes(bytes([1] * 32)).public_key()
    assert fingerprint(other) != code


def test_is_server():
    assert is_server("hermes.example.com")
    assert is_server("hermes.example.com:8443")
    assert is_server("https://hermes.example.com")
    assert not is_server("tcp/hermes.example.com:7447")
    assert not is_server("tls/[::1]:7447")
    assert server_url("hermes.example.com") == "https://hermes.example.com"
    assert server_url("http://127.0.0.1:8080/") == "http://127.0.0.1:8080"


def test_waits_for_approval_and_saves_the_credential(server, tmp_path):
    server.decisions = [{"status": "pending", "poll_after": 0}] * 2 + [approval()]
    credential, codes = run(server, tmp_path)

    directory = state_dir(server.url, tmp_path)
    key = serialization.load_pem_private_key((directory / "key.pem").read_bytes(), password=None)
    assert codes == [fingerprint(key.public_key())]
    assert server.posts[0]["hostname"] == "boxa"
    assert len(server.gets) == 3
    tls = credential.zenoh["transport"]["link"]["tls"]
    assert tls["connect_private_key"] == str(directory / "key.pem")
    assert tls["connect_certificate"] == str(directory / "cert.pem")
    assert (directory / "cert.pem").read_text() == "CERT\n"
    assert credential.zenoh["connect"]["endpoints"] == ["tls/hermes.example:7447"]
    for name in ("key.pem", "cert.pem", "ca.pem", "credential.json"):
        assert stat.S_IMODE((directory / name).stat().st_mode) == 0o600
    assert not (directory / "request.json").exists()


def test_a_saved_credential_is_used_without_asking(server, tmp_path):
    server.decisions = [approval()]
    run(server, tmp_path)
    posts, gets = len(server.posts), len(server.gets)
    credential, codes = run(server, tmp_path)
    assert (len(server.posts), len(server.gets)) == (posts, gets)
    assert codes == []
    assert not credential.expired


def test_an_expired_credential_asks_again_with_the_same_key(server, tmp_path):
    server.decisions = [approval(expires=datetime.now(timezone.utc) - timedelta(seconds=1))]
    run(server, tmp_path)
    server.decisions = [approval()]
    credential, _ = run(server, tmp_path)
    assert len(server.posts) == 2
    assert server.posts[0]["public_key"] == server.posts[1]["public_key"]
    assert not credential.expired


def test_a_saved_request_is_resumed_not_asked_again(server, tmp_path):
    class Stop(Exception):
        pass

    def interrupt(seconds):
        raise Stop                       # Ctrl+C while pending

    with pytest.raises(Stop):
        enroll(server.url, "boxa", tmp_path, sleep=interrupt)
    assert len(server.posts) == 1

    server.decisions = [approval()]
    run(server, tmp_path)
    assert len(server.posts) == 1
    assert server.gets == ["req1", "req1"]


def test_a_forgotten_request_is_asked_again(server, tmp_path):
    with pytest.raises(KeyboardInterrupt):
        enroll(server.url, "boxa", tmp_path, sleep=lambda s: (_ for _ in ()).throw(KeyboardInterrupt))
    server.forget = True
    server.decisions = [approval()]
    run(server, tmp_path)
    assert len(server.posts) == 2


@pytest.mark.parametrize("decision", ["denied", "expired"])
def test_denied_or_expired_is_an_error_and_drops_the_request(server, tmp_path, decision):
    server.decisions = [{"status": decision}]
    with pytest.raises(EnrollError, match=decision):
        run(server, tmp_path)
    assert not (state_dir(server.url, tmp_path) / "request.json").exists()


def test_refuses_a_server_whose_code_differs(server, tmp_path):
    server.code_override = "AAAA-AAAA"
    with pytest.raises(EnrollError, match="changed the key"):
        run(server, tmp_path)


def test_a_refused_request_shows_the_servers_reason(server, tmp_path):
    server.post_status = (409, "boxa is enrolled to another key")
    with pytest.raises(EnrollError, match="enrolled to another key"):
        run(server, tmp_path)


def test_retries_while_the_server_is_down(server, tmp_path):
    server.fail_next = 2
    server.decisions = [approval()]
    reasons = []
    run(server, tmp_path, on_retry=reasons.append)
    assert len(reasons) == 2 and "503" in reasons[0]


@pytest.mark.parametrize("name", ["key.pem", "credential.json", "../evil", ".hidden", "a/b"])
def test_refuses_files_it_shouldnt_write(server, tmp_path, name):
    server.decisions = [approval(files={name: "x"})]
    with pytest.raises(EnrollError, match="won't write"):
        run(server, tmp_path)
    assert not (state_dir(server.url, tmp_path) / "credential.json").exists()


def test_refuses_a_reference_to_a_file_not_sent(server, tmp_path):
    body = approval()
    body["files"] = {"ca.pem": "CA\n"}
    server.decisions = [body]
    with pytest.raises(EnrollError, match="didn't send"):
        run(server, tmp_path)


@pytest.mark.parametrize("hostname", ["", "a/b", "box*", "@box", "box$x"])
def test_refuses_hostnames_that_arent_one_key_chunk(server, tmp_path, hostname):
    with pytest.raises(EnrollError, match="can't be enrolled"):
        run(server, tmp_path, hostname=hostname)
    assert server.posts == []


def test_share_merges_the_servers_endpoints_with_its_own(server, tmp_path):
    from synq_expose.cli import enroll_with_servers
    server.decisions = [approval()]
    endpoints, layers = enroll_with_servers(["tcp/relay:7447", server.url])
    assert endpoints == ["tcp/relay:7447", "tls/hermes.example:7447"]
    assert "connect" not in layers[0]
    assert layers[0]["transport"]["link"]["tls"]["connect_certificate"].endswith("cert.pem")
