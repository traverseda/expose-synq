# expose-synq

> **Prototype.** This is a personal experiment, not one of Spiri's product offerings, and it isn't supported.

Expose a local folder to remote AI agents over [SpiriSynq](https://pypi.org/project/SpiriSynq/). The remote can read files, and optionally write files and run commands in a bubblewrap sandbox.

```bash
expose-synq share ./ -w -x                      # read/write + sandboxed exec on <hostname>/expose/<dir>
expose-synq share ./ -w -x --no-sandbox         # exec runs unconfined, as you
expose-synq share ~/proj -x --confirm           # approve each write and command
expose-synq share . -w -c tcp/relay:7447
expose-synq share . --public                    # findable by any zenoh peer on the network
expose-synq mount host/expose/proj ~/mnt -c tcp/host:7447   # the share as a local filesystem (FUSE)
```

Only reading is allowed unless you ask for more. Each capability is its own object, created only when granted:

```
<topic>                 Workspace     what's exposed and what's granted
<topic>/fs/read         FsRead        always
<topic>/fs/write        FsWrite       --allow-write
<topic>/shell           Shell         --allow-execute
<root>/tickets/@<secret>              byte streams: file reads/writes, processes
```

The filesystem RPCs follow the Unix calls they're named after. They act on symlinks, never through them, and skip or refuse excluded paths:

```
FsRead   stat(path)                     lstat → {type, size, mtime, mode}
         list_dir(path)                 streams {name, type, size, mtime, mode}, by name
         glob(path, pattern)            streams paths; pattern relative to path, ** for any depth
         grep(path, regex)              streams {path, line, text}; binary files skipped
         hash(path, algorithm)          "sha256:<hex>"
         open_read(path)                read ticket
FsWrite  open_write(path, truncate, exclusive=False)    write ticket; exclusive is O_EXCL
         edit_file(path, old, new)      replace exactly one match, by temp file + rename (sed -i)
         rename(path, to)               rename(2): atomic, replaces to; EXDEV across mounts
         chmod(path, mode)
         mkdir(path, parents=False)     parents: mkdir -p
         delete(path, recursive=False)  unlink, rmdir, or rm -r
```

`delete(recursive=True)` refuses a tree holding excluded or read-only paths before removing anything, since the caller can't see what it would destroy. Directory patterns match directories as in `.gitignore`, so `.git/**` makes `.git` itself read-only.

File contents and process output never travel through SpiriSynq RPCs. `open_read`, `open_write` and `spawn` return a **ticket**: a few raw zenoh queryables under `<root>/tickets/@<secret>`, where `<root>` is the node's base topic. Each ticket's stream protocol is in the docstrings of `FileReadStream`, `FileWriteStream` and `ProcessStreams`.

A ticket is an open handle. It lasts until `<t>/close`, or until it goes `ttl` seconds (returned by `open_read`/`open_write`) without a request. Every ticket has `<t>/heartbeat` for a client that holds one without using it. Work the server does on its own, like publishing a stream, doesn't count, so a client that disappears takes its streams with it. `client.stream` and `client.RemoteFile` heartbeat for as long as they're in use, every third of the ttl the heartbeat reply reports.

### Ticket routes

`@t` is the ticket key. `route/{a, b?}` is a zenoh query on `@t/route` with selector parameters `a` and optionally `b` (`@t/write?offset=0`), and `data` is the query's payload. Unless noted, a route replies `{}` (JSON) or an error.

**Read ticket**, from `FsRead.open_read(path)` → `{ticket, size, mtime, ttl}`:

```
@t/read/{start, end?, chunk?, readahead?}    → raw byte replies, one per chunk, then the query ends.
                                               Always at least one reply (empty past EOF). The slow path.
@t/stream/{id, start, end?, chunk?, readahead?, max_rate?}
                                             → {} once started; the bytes go to @t/data/{id}
@t/stream/{stop, id}                         → stops stream id
@t/stream/{rate, id, max_rate}               → changes a running stream's max_rate
@t/data/{id}                                   published samples, told apart by their attachment:
                                                 o=<offset>   data at that offset
                                                 end=<offset> empty, the stream is done
                                                 err=<message> empty, the stream failed
@t/close                                     → stops every stream; later reads are refused
```

- `end` is exclusive. Left out, a read or stream goes to the current end of the file.
- `chunk` is in bytes, clamped to 4 KiB–4 MiB (default 1 MiB).
- `readahead` is `auto` (default), `on` or `off`. With `auto`, read-ahead starts after the first few chunks.
- `max_rate` is in bytes per second; 0 means no limit. While it's set, each chunk is at most one second's worth.

How a client streams:

1. It picks `id` and subscribes to `@t/data/{id}`, then sends the `stream` request.
2. It checks each sample's offset against what it expects next.
3. If it finds a gap, or nothing arrives for a while, or its buffer overflows, it sends `stop` and streams again from its last offset.

There's no backpressure: zenoh's applies to the whole link, so one slow consumer would stall everything else on the session. Instead, the client controls the stream's rate, much as TCP does:

- **Buffer overflow** (the consumer fell behind): restart at 90% of the consumer's pace. That pace is the median over its last 32 samples, so a single pause doesn't count.
- **Gap** (samples lost): restart at half the rate.
- **No loss:** every half second the client raises the rate 1.5× with `rate`, without restarting. Past 8 GiB/s the limit is lifted, or returns to the caller's own `max_rate` if it set one.

A consumer that keeps up never meets a limit.

The file may grow, as with `tail -f`: to follow it, stream again from where the last stream ended. A read or stream fails with `FileChangedError` if the file is replaced (a different inode at the path, which is what most editors do on save) or truncated (smaller than the ticket has seen it). Call `open_read` again for the new file. Bytes rewritten in place aren't detected, as with any Unix reader. To check a read, hash what you received and compare it with `FsRead.hash(path)`.

**Write ticket**, from `FsWrite.open_write(path, truncate, exclusive)` → `{ticket, size, ttl}`:

```
@t/write/{offset, data}    → pwrite data at offset
@t/append/{seq, data}      → write data at the end of the file (O_APPEND)
@t/truncate/{size}         → ftruncate: cut the file to size bytes, or extend it with zeros
@t/flush                   → fsync
@t/close                   → closes the file
```

- `write` lands where it says, in any order. Resending one is harmless, so a client keeps many in flight and resends any that go unacked.
- `append` needs the ack before the next one goes out. `seq` counts the ticket's appends from 0. A resend of one that landed is acked without being written twice, and a `seq` further ahead than the next one is refused.
- Writes land in the file as they arrive. There's no temp file, rename or commit.
- `truncate`, `flush` and `close` can be repeated. `RemoteFile.truncate` waits for its writes in flight first, so it lands after them, as for one Unix writer. After `close` the ticket keeps answering repeats for 30 s, then it goes away.

**Every ticket**, process tickets included:

```
@t/heartbeat               → {ttl}. Counts as use, nothing else
```

Find exposed workspaces with `synq topic list --type Workspace`.

## Mounting

`expose-synq mount TOPIC MOUNTPOINT` mounts a share with FUSE (libfuse 3, through mfusepy), until Ctrl+C or `fusermount3 -u MOUNTPOINT`. It takes the same zenoh options as `share`. A share without `--allow-write` mounts read-only. On NixOS, point `FUSE_LIBRARY_PATH` at `libfuse3.so`.

Each FUSE call is the RPC of the same name, and an open file is a ticket: a read ticket, a write ticket, or both for `O_RDWR`. A remote error's errno reaches the program as is: excluded paths are ENOENT, read-only ones EROFS.

- Writes return once they're sent. `close(2)` and `fsync` wait for them, so a failed write shows up there at the latest, as on NFS.
- The mount sees its own writes: `stat` and reads wait for its writes in flight to that path first.
- A file truncated or replaced by someone else fails a read with EIO (the ticket's `FileChangedError`). Truncates through the same mount don't.
- There are no RPCs for `readlink`, `symlink`, `link`, `chown` or timestamps, so those fail with ENOSYS. Symlinks are listed but can't be followed, and `touch` on an existing file fails.

## Limitation: no access control

This prototype has **no access control**. expose-synq doesn't check who's calling, and it doesn't configure or rely on Zenoh `access_control`. Anyone who can reach the node over Zenoh can call every capability you granted, including `--allow-execute`. Only run it on networks you trust. The default config has no connectivity, while `--public` makes it reachable by every Zenoh peer on the network.

Tickets (`<root>/tickets/@<secret>`) can't be found by wildcard queries or subscriptions, because Zenoh wildcards never match an `@` segment. But a ticket is a bearer secret: anyone who learns its key can use it.

Who can reach the node is zenoh's job: TLS and `transport/auth` in its config (see below). Within the workspace, `PathPolicy` keeps paths inside the root (no `..`, no symlinks along the way) and applies the excludes and read-only paths.

## Zenoh configuration

By default expose-synq doesn't listen, connect or scout at all. You open up connectivity, and secure it, through zenoh's own config: TLS, `transport/auth` and `access_control`. Each layer below overrides the ones above it:

1. the locked-down base; `--public` adds zenoh's normal multicast and gossip scouting on top. It opens no listener: peers are reached by connecting out to the listeners they advertise.
2. a JSON5 config file: `--zenoh-config FILE`, `EXPOSE_SYNQ_ZENOH_CONFIG` or `ZENOH_CONFIG`
3. one environment variable per key: `EXPOSE_SYNQ_ZENOH__` followed by the key path, with `__` between segments
4. `-Z KEY=VALUE` flags
5. `--connect` / `--listen`

Values are parsed as JSON5 and fall back to a plain string. Dicts merge key by key, and any other value replaces what was there.

```bash
# scout and listen on one interface only
export EXPOSE_SYNQ_ZENOH__SCOUTING__MULTICAST__ENABLED=true
export EXPOSE_SYNQ_ZENOH__SCOUTING__MULTICAST__INTERFACE=eth1
export EXPOSE_SYNQ_ZENOH__LISTEN__ENDPOINTS='["tcp/10.0.0.5:0"]'
expose-synq share . -w
```

## Development

**Design rule:** we are not smarter than the people who made Unix. We can add niceties and change names, but we live with the same physical limits they did, so the correct way to do something is correct both on Unix and on expose-synq. For example, rewriting a file that others may be reading is done by writing a new file and renaming it over the old one, here as on Unix. It's not something tickets guarantee.

```bash
uv sync
uv run pytest
uv run python bench/bench.py [--quick] [--json out.json]
uv run expose-synq share --help
```

Tests and benchmarks run a real two-node network: the share on one SpiriSynq session, the agent on another, joined over TCP on loopback with scouting off (`synq_expose.testing`). The agent side uses SpiriSynq mirrors for RPCs and `synq_expose.client` for ticket streams. Behaviour that's specified but still a stub is tested under a strict `xfail`, so implementing it makes the test fail until the marker is removed. Sandbox tests skip when `bwrap` isn't installed, and mount tests without FUSE (`/dev/fuse`, `fusermount3`, libfuse).

The benchmark's file cases go layer by layer, with each layer's share of native file I/O: native, zenoh serving the same bytes from memory, then the ticket. Run it after every change to the data path; the layer whose share drops is where the regression is. It runs on tmpfs, since disk writeback makes the native baseline swing. Both nodes share one process (and the GIL) there; for numbers that matter, run a real `expose-synq share` and a client in separate processes.

To see where a layer's time goes, profile the server with py-spy (0.4.2 or later for Python 3.14): `uvx py-spy@0.4.2 record --gil --format raw -o out.txt -- .venv/bin/python …`. `--gil` keeps only samples that hold the GIL; compare it with a run without it to see what's blocking versus what's waiting.

Layout (`src/synq_expose/`):

- `cli.py`: the Typer app (`share` and `mount`); validates flags into an `ExposeConfig` and serves
- `mount.py`: `SynqFS`, the FUSE operations over the RPCs and tickets
- `config.py`: `ExposeConfig` and friends, plus the layered zenoh config and `reopen_session()`
- `workspace.py`: the `Workspace` object, and `build_objects()`, which creates one object per granted capability
- `tickets.py`: `TicketRegistry`, which creates, serves and revokes tickets
- `fs.py`: `FsRead`, `FsWrite`, and the file read/write streams
- `shell.py`: `Shell`, `ProcessStreams` (pull-based output by offset, after traverseda/zenoh-fs)
- `policy.py`: path confinement, excludes, read-only globs
- `guard.py`: `--confirm` and the audit log
- `sandbox.py`: bubblewrap argv
- `client.py`: the agent side of the ticket streams
- `testing.py`: the loopback test network
