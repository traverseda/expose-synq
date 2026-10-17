"""Network interfaces to bind zenoh to, with ZeroTier network names where we can get them."""
import json
import os
import subprocess
import urllib.error
import urllib.request
from dataclasses import dataclass, replace
from pathlib import Path

ZEROTIER_HOME = Path("/var/lib/zerotier-one")
ZEROTIER_PORT = 9993


@dataclass(frozen=True)
class ZeroTierNetwork:
    id: str
    name: str
    device: str
    addresses: tuple[str, ...] = ()
    """``ip/prefix`` as the controller assigned them."""
    status: str = ""


@dataclass(frozen=True)
class Interface:
    name: str
    addresses: tuple[str, ...] = ()
    """Bare IPs, IPv4 first; no link-local IPv6 (it can't be bound without a scope id)."""
    up: bool = True
    zerotier: ZeroTierNetwork | None = None

    @property
    def is_zerotier(self) -> bool:
        # ZeroTier names its Linux devices zt<hash of the network id>.
        return self.zerotier is not None or self.name.startswith("zt")

    @property
    def label(self) -> str:
        if self.zerotier is not None:
            return f"ZeroTier {self.zerotier.name or '(unnamed)'} [{self.zerotier.id}]"
        if self.is_zerotier:
            return "ZeroTier (network name unknown)"
        return ""


class ZeroTierUnavailable(Exception):
    pass


def parse_ip_json(raw: str) -> list[Interface]:
    """Interfaces from ``ip -j addr show``."""
    interfaces = []
    for link in json.loads(raw):
        v4, v6 = [], []
        for addr in link.get("addr_info", []):
            if addr.get("family") == "inet":
                v4.append(addr["local"])
            elif addr.get("family") == "inet6" and addr.get("scope") != "link":
                v6.append(addr["local"])
        flags = link.get("flags", [])
        interfaces.append(Interface(link["ifname"], tuple(v4 + v6), up="UP" in flags))
    return interfaces


def list_interfaces() -> list[Interface]:
    """Every interface with an address, via iproute2 (Linux)."""
    try:
        out = subprocess.run(["ip", "-j", "addr", "show"], capture_output=True, text=True, check=True).stdout
    except (OSError, subprocess.CalledProcessError) as e:
        raise RuntimeError(f"can't list network interfaces with `ip -j addr show`: {e}") from e
    return [i for i in parse_ip_json(out) if i.addresses]


def parse_zerotier_networks(raw: str | bytes) -> list[ZeroTierNetwork]:
    """Networks from ZeroTier's ``GET /network``, or ``zerotier-cli -j listnetworks`` (same JSON)."""
    return [
        ZeroTierNetwork(
            id=n.get("id") or n.get("nwid", ""),
            name=n.get("name", ""),
            device=n.get("portDeviceName", ""),
            addresses=tuple(n.get("assignedAddresses", [])),
            status=n.get("status", ""),
        )
        for n in json.loads(raw)
    ]


def zerotier_token(home: Path = ZEROTIER_HOME) -> str | None:
    """The local service's auth token, looked for where zerotier-cli looks for it."""
    for path in (Path.home() / ".zeroTierOneAuthToken", home / "authtoken.secret"):
        try:
            return path.read_text().strip()
        except OSError:
            continue
    return None


def zerotier_port(home: Path = ZEROTIER_HOME) -> int:
    try:
        return int((home / "zerotier-one.port").read_text())
    except (OSError, ValueError):
        return ZEROTIER_PORT


def zerotier_networks(token: str | None = None, port: int | None = None, timeout: float = 2.0) -> list[ZeroTierNetwork]:
    """Joined networks from the local ZeroTier service's API. Raises ZeroTierUnavailable."""
    token = token or os.environ.get("ZEROTIER_AUTH_TOKEN") or zerotier_token()
    if not token:
        raise ZeroTierUnavailable(
            "no ZeroTier auth token (it's in /var/lib/zerotier-one/authtoken.secret, readable by root only)"
        )
    request = urllib.request.Request(
        f"http://127.0.0.1:{port or zerotier_port()}/network", headers={"X-ZT1-Auth": token},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return parse_zerotier_networks(response.read())
    except urllib.error.HTTPError as e:
        raise ZeroTierUnavailable(f"the ZeroTier service refused the token ({e.code})") from e
    except (urllib.error.URLError, OSError) as e:
        raise ZeroTierUnavailable(f"can't reach the ZeroTier service: {e}") from e


def zerotier_networks_sudo() -> list[ZeroTierNetwork]:
    """``sudo zerotier-cli -j listnetworks``; sudo may prompt on the terminal."""
    try:
        out = subprocess.run(["sudo", "zerotier-cli", "-j", "listnetworks"], stdout=subprocess.PIPE, check=True).stdout
        return parse_zerotier_networks(out)
    except (OSError, subprocess.CalledProcessError, ValueError) as e:
        raise ZeroTierUnavailable(f"sudo zerotier-cli failed: {e}") from e


def with_zerotier(interfaces: list[Interface], networks: list[ZeroTierNetwork]) -> list[Interface]:
    """Attach each network to its device, ZeroTier interfaces first."""
    by_device = {n.device: n for n in networks}
    named = [replace(i, zerotier=by_device.get(i.name, i.zerotier)) for i in interfaces]
    return sorted(named, key=lambda i: not i.is_zerotier)
