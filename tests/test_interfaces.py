import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from synq_expose import interfaces as netif

IP_JSON = json.dumps([
    {"ifname": "lo", "flags": ["LOOPBACK", "UP"], "addr_info": [
        {"family": "inet", "local": "127.0.0.1", "scope": "host"},
        {"family": "inet6", "local": "::1", "scope": "host"}]},
    {"ifname": "wlan0", "flags": ["BROADCAST", "UP"], "addr_info": [
        {"family": "inet", "local": "192.168.0.102", "scope": "global"},
        {"family": "inet6", "local": "fe80::1", "scope": "link"}]},
    {"ifname": "zt6jyrztxd", "flags": ["BROADCAST", "MULTICAST", "UP"], "addr_info": [
        {"family": "inet6", "local": "fd00::68", "scope": "global"},
        {"family": "inet", "local": "192.168.193.68", "scope": "global"}]},
    {"ifname": "docker0", "flags": ["NO-CARRIER"], "addr_info": [
        {"family": "inet", "local": "172.17.0.1", "scope": "global"}]},
    {"ifname": "eth9", "flags": ["UP"], "addr_info": []},
])

NETWORKS_JSON = json.dumps([{
    "id": "8056c2e21c000001", "name": "homelab", "portDeviceName": "zt6jyrztxd",
    "assignedAddresses": ["192.168.193.68/24"], "status": "OK", "type": "PRIVATE",
}])


def test_parse_ip_json():
    found = {i.name: i for i in netif.parse_ip_json(IP_JSON)}
    assert found["lo"].addresses == ("127.0.0.1", "::1")
    assert found["wlan0"].addresses == ("192.168.0.102",)       # no link-local
    assert found["zt6jyrztxd"].addresses == ("192.168.193.68", "fd00::68")    # IPv4 first
    assert found["docker0"].up is False
    assert found["eth9"].addresses == ()


def test_zerotier_named_and_sorted_first():
    networks = netif.parse_zerotier_networks(NETWORKS_JSON)
    assert networks[0].name == "homelab" and networks[0].device == "zt6jyrztxd"
    merged = netif.with_zerotier(netif.parse_ip_json(IP_JSON), networks)
    assert merged[0].name == "zt6jyrztxd"
    assert merged[0].label == "ZeroTier homelab [8056c2e21c000001]"
    assert [i.name for i in merged[1:]] == ["lo", "wlan0", "docker0", "eth9"]


def test_zerotier_without_names_is_still_recognised():
    merged = netif.with_zerotier(netif.parse_ip_json(IP_JSON), [])
    assert merged[0].name == "zt6jyrztxd"
    assert merged[0].label == "ZeroTier (network name unknown)"


@pytest.fixture
def zerotier_service():
    """A stand-in for the ZeroTier service's local API, accepting token 'good'."""
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.headers.get("X-ZT1-Auth") != "good":
                self.send_response(401)
                self.end_headers()
                return
            body = NETWORKS_JSON.encode() if self.path == "/network" else b"{}"
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield server.server_address[1]
    server.shutdown()


def test_zerotier_api(zerotier_service):
    networks = netif.zerotier_networks(token="good", port=zerotier_service)
    assert [(n.name, n.device) for n in networks] == [("homelab", "zt6jyrztxd")]


def test_zerotier_api_bad_token(zerotier_service):
    with pytest.raises(netif.ZeroTierUnavailable, match="refused"):
        netif.zerotier_networks(token="bad", port=zerotier_service)


def test_zerotier_token_lookup(tmp_path, monkeypatch):
    monkeypatch.setattr(netif.Path, "home", lambda: tmp_path)
    assert netif.zerotier_token(home=tmp_path / "zt") is None
    (tmp_path / ".zeroTierOneAuthToken").write_text("abc\n")
    assert netif.zerotier_token(home=tmp_path / "zt") == "abc"


def test_zerotier_no_token(monkeypatch):
    monkeypatch.delenv("ZEROTIER_AUTH_TOKEN", raising=False)
    monkeypatch.setattr(netif, "zerotier_token", lambda: None)
    with pytest.raises(netif.ZeroTierUnavailable, match="token"):
        netif.zerotier_networks()
