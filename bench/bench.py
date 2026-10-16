"""Benchmarks over the two-node loopback network (synq_expose.testing).

    uv run python bench/bench.py              # full run
    uv run python bench/bench.py --quick      # small sizes, a few seconds
    uv run python bench/bench.py --json out.json

Each case prints one line. A case that hits a stub reports "not implemented"
instead of failing the run.

File cases go layer by layer: native file I/O, zenoh serving the same bytes
from RAM, then the ticket, each with its share of the native rate. "copy" is
a ticket read written into a local file, against a native file copy. Run it after every change to the data path: a layer whose
share drops is where the regression is.

Both nodes run in this one process, so they share the GIL. Measured as two
processes, reads and writes come out within ~10% of these numbers.

The ``raw`` cases serve bytes from a trivial ticket handler at several chunk
sizes, to show which one the transport prefers.
"""
import argparse
import json
import os
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path

from loguru import logger

from synq_expose.testing import Network, wait_for  # first: locks down the default session

from synq_expose import client
from synq_expose.config import ExecConfig, ExposeConfig, SandboxConfig
from synq_expose.fs import CHUNK_SIZE, FsRead, FsWrite
from synq_expose.shell import Shell

KiB, MiB = 1 << 10, 1 << 20


@dataclass
class Result:
    name: str
    unit: str = ""
    values: dict = field(default_factory=dict)
    error: str | None = None

    def line(self) -> str:
        if self.error:
            return f"{self.name:<34} {self.error}"
        return f"{self.name:<34} " + "  ".join(f"{k} {v:,.2f}" for k, v in self.values.items()) + f" {self.unit}"


def latency(name: str, fn, n: int) -> Result:
    fn()  # warm up
    samples = []
    for _ in range(n):
        start = time.perf_counter()
        fn()
        samples.append((time.perf_counter() - start) * 1000)
    samples.sort()
    pct = lambda p: samples[min(len(samples) - 1, int(p / 100 * len(samples)))]
    return Result(name, "ms", {"p50": pct(50), "p90": pct(90), "p99": pct(99), "max": samples[-1]})


def throughput(name: str, fn, nbytes: int, repeat: int = 3) -> Result:
    """Best of ``repeat`` runs of ``fn``, which moves ``nbytes``."""
    best = min(timed(fn) for _ in range(repeat))
    return Result(name, "MiB/s", {"rate": nbytes / MiB / best, "secs": best})


def timed(fn) -> float:
    start = time.perf_counter()
    fn()
    return time.perf_counter() - start


def guarded(name: str, case) -> Result:
    try:
        return case()
    except Exception as e:
        # Stubs raise a bare NotImplementedError. SpiriSynq reports it as "RPC error ''".
        stub = "NotImplementedError" in str(e) or str(e).endswith("RPC error ''")
        return Result(name, error="not implemented" if stub else f"error: {e}")


# ── Cases ───────────────────────────────────────────────────────────────────

def bench_rpc(net: Network, share, n: int) -> list[Result]:
    (share.workspace / "tiny.txt").write_text("x")
    fs = share.mirror(FsRead, "/fs/read")
    return [
        guarded("rpc hash (tiny file)", lambda: latency("rpc hash (tiny file)", lambda: fs.hash("tiny.txt"), n)),
        guarded("rpc open_read (tiny file)", lambda: latency("rpc open_read (tiny file)", lambda: fs.open_read("tiny.txt"), n)),
    ]


def bench_mint(share, n: int) -> list[Result]:
    def mint_revoke():
        share.tickets.revoke(share.tickets.mint("bench", {"": lambda q: None}).key)
    return [latency("ticket mint+revoke (local)", mint_revoke, n)]


def mint_remote(net: Network, share, route: str, handler) -> str:
    """Mint a ticket on the server and wait until the client can reach it.

    An RPC that mints a ticket doesn't need this: its declaration goes out on
    the same link ahead of the reply carrying the key. Minting locally and
    querying straight away races it.
    """
    key = share.tickets.mint("bench", {route: handler}).key
    wait_for(net.z, f"{key}/{route}")
    return key


def bench_raw(net: Network, share, total: int) -> list[Result]:
    results = []
    blob = os.urandom(4 * MiB)
    for chunk in (64 * KiB, 256 * KiB, 1 * MiB, 4 * MiB):
        chunk = min(chunk, total)

        def serve(query, chunk=chunk):
            for offset in range(0, total, chunk):
                query.reply(query.key_expr, blob[:min(chunk, total - offset)])

        t = mint_remote(net, share, "read", serve)
        name = f"raw download {chunk // KiB}KiB chunks"
        results.append(guarded(name, lambda: throughput(name, lambda: drain(net, t, total), total)))

        u = mint_remote(net, share, "write", lambda q: q.reply(q.key_expr, b"{}"))
        name = f"raw upload {chunk // KiB}KiB chunks"
        data = blob[:chunk]
        results.append(guarded(name, lambda: throughput(
            name, lambda: [client.call(net.z, f"{u}/write?offset={i}", payload=data) for i in range(0, total, chunk)], total)))
    return results


def drain(net: Network, ticket: str, expected: int) -> None:
    got = sum(len(c) for c in client.read_chunks(net.z, ticket))
    assert got == expected, f"got {got} bytes, expected {expected}"


def native_read(path: Path) -> None:
    with path.open("rb", buffering=0) as f:
        while f.read(CHUNK_SIZE):
            pass


def native_write(path: Path, data: bytes) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC)
    try:
        view = memoryview(data)
        for offset in range(0, len(data), CHUNK_SIZE):
            os.pwrite(fd, view[offset:offset + CHUNK_SIZE], offset)
    finally:
        os.close(fd)


def native_copy(source: Path, dest: Path) -> None:
    with source.open("rb", buffering=0) as src, dest.open("wb", buffering=0) as dst:
        while chunk := src.read(CHUNK_SIZE):
            dst.write(chunk)


def layered(name: str, fn, nbytes: int, native: Result | None) -> Result:
    """``throughput``, plus the share of the native rate where there is one."""
    result = throughput(name, fn, nbytes)
    if native is not None and not native.error:
        result.values["native%"] = 100 * result.values["rate"] / native.values["rate"]
    return result


def bench_files(net: Network, share, sizes: list[int], local: Path) -> list[Result]:
    """Each size, layer by layer from native file I/O up, so every run shows where the cost is.

    Reads stream: chunks are consumed as they arrive, never collected.
    Native runs use the same chunk size, through the page cache, without fsync.
    """
    results = []
    fs_read = share.mirror(FsRead, "/fs/read")
    fs_write = share.mirror(FsWrite, "/fs/write")
    for size in sizes:
        label = f"{size // MiB}MiB" if size >= MiB else f"{size // KiB}KiB"
        data = os.urandom(size)
        source = share.workspace / "file.bin"
        source.write_bytes(data)

        name = f"open_read {label}"
        results.append(guarded(name, lambda: latency(name, lambda: fs_read.open_read("file.bin"), 10)))
        name = f"rpc hash {label}"
        results.append(guarded(name, lambda: latency(name, lambda: fs_read.hash("file.bin"), 10)))

        nr = throughput(f"read {label}: native", lambda: native_read(source), size)
        results.append(nr)

        # The file's own bytes, already in memory, sliced fresh for every
        # chunk: one buffer resent over and over stays in the CPU cache and
        # makes zenoh look about twice as fast as it is with real data.
        def serve(query):
            for offset in range(0, size, CHUNK_SIZE):
                query.reply(query.key_expr, data[offset:offset + CHUNK_SIZE])
        t = mint_remote(net, share, "read", serve)
        name = f"read {label}: zenoh, from RAM"
        results.append(guarded(name, lambda: layered(name, lambda: drain(net, t, size), size, nr)))

        def stream():
            return client.stream(net.z, fs_read.open_read("file.bin")["ticket"])

        def ticket_read():
            drain(net, fs_read.open_read("file.bin")["ticket"], size)
        name = f"read {label}: ticket read (slow path)"
        results.append(guarded(name, lambda: layered(name, ticket_read, size, nr)))

        def ticket_stream():
            got = sum(len(c) for c in stream())
            assert got == size, f"got {got} bytes, expected {size}"
        name = f"read {label}: ticket stream"
        results.append(guarded(name, lambda: layered(name, ticket_stream, size, nr)))

        nc = throughput(f"copy {label}: native", lambda: native_copy(source, local / "copy.bin"), size)
        results.append(nc)

        def download():
            with (local / "download.bin").open("wb", buffering=0) as f:
                for piece in stream():
                    f.write(piece)
        name = f"copy {label}: ticket to a local file"
        results.append(guarded(name, lambda: layered(name, download, size, nc)))

        nw = throughput(f"write {label}: native", lambda: native_write(local / "native.bin", data), size)
        results.append(nw)

        def write():
            client.write_all(net.z, fs_write.open_write("out.bin", truncate=True)["ticket"], data)
        name = f"write {label}: ticket"
        results.append(guarded(name, lambda: layered(name, write, size, nw)))
    return results


def bench_spawn(net: Network, share, n: int) -> list[Result]:
    shell = share.mirror(Shell, "/shell")

    def run_true():
        t = shell.spawn(["true"])
        while client.call_json(net.z, f"{t}/status")["running"]:
            time.sleep(0.001)
        client.call(net.z, f"{t}/discard")
    return [guarded("spawn `true` to exit (unsandboxed)", lambda: latency("spawn `true` to exit (unsandboxed)", run_true, n))]


def main(argv: list[str] | None = None) -> list[Result]:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--quick", action="store_true", help="small sizes and few iterations")
    parser.add_argument("--json", type=Path, help="also write results here")
    args = parser.parse_args(argv)

    n = 20 if args.quick else 200
    raw_total = 4 * MiB if args.quick else 64 * MiB
    sizes = [64 * KiB, 1 * MiB] if args.quick else [64 * KiB, 1 * MiB, 64 * MiB, 256 * MiB]

    results = []
    net = Network.open()
    # tmpfs when there is one: on a disk, writeback makes the native baseline
    # swing with whatever ran before it, which hides our own overhead.
    scratch = "/dev/shm" if os.path.isdir("/dev/shm") else None
    with tempfile.TemporaryDirectory(dir=scratch) as tmp, tempfile.TemporaryDirectory(dir=scratch) as local:
        workspace = Path(tmp)
        config = ExposeConfig(
            workspace=workspace, topic="expose/bench", allow_write=True, allow_execute=True,
            exec=ExecConfig(sandbox=SandboxConfig(enabled=False)),
        )
        share = net.share(config)
        try:
            for case in (
                lambda: bench_rpc(net, share, n),
                lambda: bench_mint(share, n),
                lambda: bench_raw(net, share, raw_total),
                lambda: bench_files(net, share, sizes, Path(local)),
                lambda: bench_spawn(net, share, n // 4),
            ):
                for result in case():
                    print(result.line(), flush=True)
                    results.append(result)
        finally:
            share.close()
            net.close()

    if args.json:
        args.json.write_text(json.dumps(
            {"chunk_size": CHUNK_SIZE, "results": [r.__dict__ for r in results]}, indent=2))
    return results


if __name__ == "__main__":
    logger.remove()
    logger.add(sys.stderr, level="CRITICAL")  # failures show in the result lines
    main()
