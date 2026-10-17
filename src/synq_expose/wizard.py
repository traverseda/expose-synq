"""``expose-synq setup``: write a zenoh config that binds to chosen interfaces and turns on zenoh's auth.

The flows are the ones zenoh 1.x can do from its config file:

- ``transport/auth/usrpwd``: a username and password, checked against a dictionary file
- ``transport/link/tls``: TLS (or QUIC) links, verifying the listener against a CA,
  and with ``enable_mtls`` the connecting side too

``transport/auth/pubkey`` is left out: zenoh reads ``known_keys_file`` into its config
but never into the authenticator, so every key is rejected unless added from Rust.
"""
import datetime
import ipaddress
import json
import os
import secrets
import socket
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import click
import typer
from rich.console import Console
from rich.table import Table

from synq_expose import interfaces as netif
from synq_expose.config import ZenohConfigError, build_zenoh_config, default_zenoh_config_path

PROTOCOLS = ("tcp", "tls", "quic")
EXISTING_FILE = click.Path(exists=True, dir_okay=False, resolve_path=True, path_type=Path)
AUTH_CHOICES = {
    "none": "no extra auth: anyone who can reach the chosen interfaces (on ZeroTier: members of the network)",
    "password": "a shared username and password (transport/auth/usrpwd)",
    "mtls": "certificates from one CA, both ends verified (TLS or QUIC links)",
    "mtls+password": "both of the above",
    "tls": "encrypted links that verify the listener only; anyone can still connect",
}


@dataclass(frozen=True)
class TlsFiles:
    ca: Path
    cert: Path
    key: Path


@dataclass(frozen=True)
class Password:
    user: str
    password: str
    dictionary: Path


@dataclass(frozen=True)
class Plan:
    listen: tuple[str, ...]
    """IPs to listen on."""
    port: int = 0
    scout_interface: str | None = None
    """Multicast scouting on this interface; None turns it off."""
    connect: tuple[str, ...] = ()
    """Endpoints, or ``host:port`` (given ``protocol``)."""
    protocol: str = "tcp"
    tls: TlsFiles | None = None
    mtls: bool = False
    password: Password | None = None


def endpoint(protocol: str, host: str, port: int | str) -> str:
    if ":" in host:     # IPv6
        host = f"[{host.strip('[]')}]"
    return f"{protocol}/{host}:{port}"


def peer_endpoint(protocol: str, peer: str) -> str:
    """``10.0.0.2:7447`` → ``tcp/10.0.0.2:7447``; an endpoint with its own protocol is kept as is."""
    return peer if "/" in peer else f"{protocol}/{peer}"


def build_config(plan: Plan) -> dict[str, Any]:
    """The zenoh config for ``plan``, as a layer over the locked-down base."""
    if plan.protocol not in PROTOCOLS:
        raise ValueError(f"protocol must be one of {PROTOCOLS}, not {plan.protocol!r}")
    if (plan.protocol != "tcp" or plan.mtls) and plan.tls is None:
        raise ValueError(f"{plan.protocol}{' with mTLS' if plan.mtls else ''} needs certificates")
    config: dict[str, Any] = {
        "listen": {"endpoints": [endpoint(plan.protocol, ip, plan.port) for ip in plan.listen]},
        "connect": {"endpoints": [peer_endpoint(plan.protocol, p) for p in plan.connect]},
        "scouting": {
            "multicast": {"enabled": plan.scout_interface is not None},
            # Peers learn of each other through the ones they're connected to.
            "gossip": {"enabled": True},
        },
    }
    if plan.scout_interface is not None:
        config["scouting"]["multicast"]["interface"] = plan.scout_interface
    transport: dict[str, Any] = {}
    if plan.tls is not None:
        tls = {
            "root_ca_certificate": str(plan.tls.ca),
            "listen_certificate": str(plan.tls.cert),
            "listen_private_key": str(plan.tls.key),
            "connect_certificate": str(plan.tls.cert),
            "connect_private_key": str(plan.tls.key),
            "enable_mtls": plan.mtls,
        }
        transport["link"] = {"tls": tls}
    if plan.password is not None:
        transport["auth"] = {"usrpwd": {
            "user": plan.password.user,
            "password": plan.password.password,
            "dictionary_file": str(plan.password.dictionary),
        }}
    if transport:
        config["transport"] = transport
    return config


def render(config: dict[str, Any]) -> str:
    stamp = datetime.date.today().isoformat()
    return (f"// Written by `expose-synq setup` on {stamp}. Layered over expose-synq's locked-down base:\n"
            "// keys not set here stay off (see README, \"Zenoh configuration\").\n"
            + json.dumps(config, indent=2) + "\n")


# ── Files ───────────────────────────────────────────────────────────────────

def write_private(path: Path, data: bytes | str) -> None:
    """Write ``path`` readable by its owner only."""
    if isinstance(data, str):
        data = data.encode()
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    os.fchmod(fd, 0o600)
    with os.fdopen(fd, "wb") as f:
        f.write(data)


def write_dictionary(path: Path, user: str, password: str) -> None:
    """Set ``user``'s line in a usrpwd dictionary (``user:password`` per line), keeping the others."""
    try:
        lines = path.read_text().splitlines()
    except FileNotFoundError:
        lines = []
    lines = [line for line in lines if line.partition(":")[0] != user]
    write_private(path, "\n".join([*lines, f"{user}:{password}"]) + "\n")


def _crypto():
    try:
        from cryptography import x509
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import ec
    except ImportError as e:
        raise RuntimeError("making certificates needs the `cryptography` package") from e
    return x509, hashes, serialization, ec


def _key_pem(key) -> bytes:
    _, _, serialization, _ = _crypto()
    return key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                             serialization.NoEncryption())


def generate_ca(cert_path: Path, key_path: Path, name: str = "expose-synq CA", days: int = 3650) -> None:
    x509, hashes, serialization, ec = _crypto()
    key = ec.generate_private_key(ec.SECP256R1())
    subject = x509.Name([x509.NameAttribute(x509.NameOID.COMMON_NAME, name)])
    now = datetime.datetime.now(datetime.timezone.utc)
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject).issuer_name(subject).public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now).not_valid_after(now + datetime.timedelta(days=days))
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .add_extension(x509.KeyUsage(digital_signature=False, content_commitment=False, key_encipherment=False,
                                     data_encipherment=False, key_agreement=False, key_cert_sign=True,
                                     crl_sign=True, encipher_only=False, decipher_only=False), critical=True)
        .sign(key, hashes.SHA256())
    )
    write_private(key_path, _key_pem(key))
    cert_path.parent.mkdir(parents=True, exist_ok=True)
    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))


def issue_cert(ca_cert: Path, ca_key: Path, cert_path: Path, key_path: Path,
               hostname: str, ips: tuple[str, ...], days: int = 1825) -> None:
    """A node certificate for ``hostname`` and ``ips``, good for both ends of a link."""
    x509, hashes, serialization, ec = _crypto()
    ca = x509.load_pem_x509_certificate(ca_cert.read_bytes())
    try:
        signer = serialization.load_pem_private_key(ca_key.read_bytes(), password=None)
    except TypeError as e:
        raise RuntimeError(f"{ca_key} is encrypted; decrypt it first") from e
    key = ec.generate_private_key(ec.SECP256R1())
    sans = [x509.DNSName(hostname), *(x509.IPAddress(ipaddress.ip_address(ip)) for ip in ips)]
    now = datetime.datetime.now(datetime.timezone.utc)
    usage = x509.ExtendedKeyUsage([x509.oid.ExtendedKeyUsageOID.SERVER_AUTH, x509.oid.ExtendedKeyUsageOID.CLIENT_AUTH])
    cert = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(x509.NameOID.COMMON_NAME, hostname)]))
        .issuer_name(ca.subject).public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now).not_valid_after(now + datetime.timedelta(days=days))
        .add_extension(x509.SubjectAlternativeName(sans), critical=False)
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(usage, critical=False)
        .sign(signer, hashes.SHA256())
    )
    write_private(key_path, _key_pem(key))
    cert_path.parent.mkdir(parents=True, exist_ok=True)
    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))


# ── Interactive ─────────────────────────────────────────────────────────────

def _numbers(raw: str, count: int) -> list[int] | None:
    try:
        picked = [int(n) for n in raw.replace(",", " ").split()]
    except ValueError:
        return None
    return picked if picked and all(1 <= n <= count for n in picked) else None


def discover_interfaces(console: Console) -> list[netif.Interface]:
    found = netif.list_interfaces()
    if not any(i.is_zerotier for i in found):
        return found
    try:
        networks = netif.zerotier_networks()
    except netif.ZeroTierUnavailable as reason:
        console.print(f"[yellow]ZeroTier network names unavailable:[/yellow] {reason}")
        networks = []
        if typer.confirm("Read them with `sudo zerotier-cli -j listnetworks`?", default=False):
            try:
                networks = netif.zerotier_networks_sudo()
            except netif.ZeroTierUnavailable as e:
                console.print(f"[yellow]{e}[/yellow]")
        if not networks:
            console.print("[dim]To skip this next time: sudo cat /var/lib/zerotier-one/authtoken.secret "
                          "> ~/.zeroTierOneAuthToken && chmod 600 ~/.zeroTierOneAuthToken[/dim]")
    return netif.with_zerotier(found, networks)


def ask_interfaces(console: Console, found: list[netif.Interface]) -> list[netif.Interface]:
    table = Table(title="Network interfaces")
    for column in ("#", "interface", "addresses", "network"):
        table.add_column(column)
    for n, i in enumerate(found, 1):
        state = "" if i.up else " [dim](down)[/dim]"
        table.add_row(str(n), i.name + state, " ".join(i.addresses), i.label)
    console.print(table)
    default = next((n for n, i in enumerate(found, 1) if i.is_zerotier and i.up), 1)
    while True:
        picked = _numbers(typer.prompt("Listen on which interfaces (numbers, comma separated)",
                                       default=str(default)), len(found))
        if picked:
            return [found[n - 1] for n in dict.fromkeys(picked)]
        console.print(f"[red]Pick numbers from 1 to {len(found)}.[/red]")


def ask_tls(console: Console, directory: Path, hostname: str, ips: tuple[str, ...]) -> TlsFiles:
    tls_dir = directory / "tls"
    ca, ca_key = tls_dir / "ca.crt", tls_dir / "ca.key"
    have_ca = ca.exists() and ca_key.exists()
    console.print("Certificates:\n"
                  "  new      create a CA here, and this node's certificate from it\n"
                  "  existing sign this node's certificate with a CA you already have (its .crt and .key)\n"
                  "  files    use a CA certificate, node certificate and key you already have")
    source = typer.prompt("Certificates", default="existing" if have_ca else "new",
                          type=click.Choice(["new", "existing", "files"]))
    if source == "files":
        return TlsFiles(
            ca=typer.prompt("CA certificate", type=EXISTING_FILE),
            cert=typer.prompt("This node's certificate", type=EXISTING_FILE),
            key=typer.prompt("This node's private key", type=EXISTING_FILE),
        )
    if source == "new":
        if have_ca and not typer.confirm(f"Replace the CA in {tls_dir}? Nodes signed by it will stop "
                                         "connecting", default=False):
            raise typer.Abort()
        generate_ca(ca, ca_key)
        console.print(f"Created a CA: [bold]{ca}[/bold] (key [bold]{ca_key}[/bold])")
    else:
        ca = typer.prompt("CA certificate", type=EXISTING_FILE, default=ca if have_ca else None)
        ca_key = typer.prompt("CA private key", type=EXISTING_FILE, default=ca_key if have_ca else None)
    cert, key = tls_dir / "node.crt", tls_dir / "node.key"
    issue_cert(ca, ca_key, cert, key, hostname, ips)
    console.print(f"Issued [bold]{cert}[/bold] for {', '.join((hostname, *ips))}")
    return TlsFiles(ca, cert, key)


def ask_password(console: Console, directory: Path) -> Password:
    while True:
        user = typer.prompt("Username", default="expose-synq")
        if ":" not in user and user.strip() == user and user:
            break
        console.print("[red]No colons or surrounding spaces.[/red]")
    password = typer.prompt("Password (empty to generate one)", default="", hide_input=True, show_default=False)
    if not password:
        password = secrets.token_urlsafe(18)
        console.print(f"Generated password: [bold]{password}[/bold] (use it on your other machines)")
    dictionary = directory / "usrpwd.txt"
    write_dictionary(dictionary, user, password)
    return Password(user, password, dictionary)


def run(output: Path | None = None, console: Console | None = None) -> Path:
    console = console or Console()
    output = (output or default_zenoh_config_path()).expanduser().resolve()
    directory = output.parent
    if output.exists() and not typer.confirm(f"{output} exists. Replace it?", default=False):
        raise typer.Abort()

    console.rule("Network")
    chosen = ask_interfaces(console, discover_interfaces(console))
    ips = tuple(ip for i in chosen for ip in i.addresses)
    port = typer.prompt("Port (0: any free port; peers find it by scouting)", default=0, type=int)
    scout = chosen[0].name
    if not typer.confirm(f"Find peers by multicast scouting on {scout}?", default=True):
        scout = None
    peers = typer.prompt("Peers to connect to, e.g. 192.168.193.10:7447 (comma separated, empty for none)",
                         default="", show_default=False)

    console.rule("Authentication")
    choices = Table.grid(padding=(0, 2))
    for name, text in AUTH_CHOICES.items():
        choices.add_row(f"  [bold]{name}[/bold]", text)
    console.print(choices)
    console.print("[dim]  (zenoh's pubkey auth is left out: it can't trust any key from a config file)[/dim]")
    auth = typer.prompt("Authentication", default="mtls", type=click.Choice(list(AUTH_CHOICES)))
    protocol = "tcp"
    tls = password = None
    if auth in ("tls", "mtls", "mtls+password"):
        protocol = typer.prompt("Link protocol", default="tls", type=click.Choice(["tls", "quic"]))
        tls = ask_tls(console, directory, socket.gethostname(), ips)
    if auth in ("password", "mtls+password"):
        password = ask_password(console, directory)

    plan = Plan(
        listen=ips, port=port, scout_interface=scout,
        connect=tuple(p.strip() for p in peers.split(",") if p.strip()),
        protocol=protocol, tls=tls, mtls=auth.startswith("mtls"), password=password,
    )
    write_private(output, render(build_config(plan)))
    try:
        build_zenoh_config(output, environ={})
    except ZenohConfigError as e:
        console.print(f"[red]zenoh rejected the config:[/red] {e}")
        raise typer.Exit(1)

    console.rule("Done")
    console.print(f"Wrote [bold]{output}[/bold]. share, mount and run use it unless given --zenoh-config.")
    if tls is not None and tls.ca.parent == directory / "tls":
        console.print(f"On each other machine, copy {tls.ca} and its .key over and pick "
                      "[bold]existing[/bold] at the certificates step.")
    if password is not None:
        console.print("On each other machine, use the same username and password.")
    return output
