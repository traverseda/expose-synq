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
expose-synq run -c tcp/host:7447 --topic host/expose/proj bash   # an interactive shell in the share
expose-synq run -c tcp/host:7447 --topic host/expose/proj make test   # or any command, with its exit status
expose-synq file pull -c tcp/host:7447 host/expose/proj:src/app.py   # one file to stdout; see "File commands"
```

Only reading is allowed unless you ask for more. Each capability is its own object, created only when granted, so what a share grants is found with SpiriSynq's discovery, by which objects are there:

```
<topic>/fs/read         FsRead        always
<topic>/fs/write        FsWrite       --allow-write
<topic>/shell           Shell         --allow-execute; its sandboxed field says whether commands run in bwrap
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

File contents and process output never travel through SpiriSynq RPCs. `open_read`, `open_write` and `spawn` return a **ticket**: a few raw zenoh queryables under `<root>/tickets/@<secret>`, where `<root>` is the node's base topic. Each ticket's stream protocol is in the docstrings of `FileReadStream`, `FileWriteStream` and `Process`.

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

**Process ticket**, from `Shell.spawn(command, stdin=False, pty=None, timeout=None)` → `{ticket, ttl, pty}`:

A `str` command runs through `--shell`, a list runs as argv. The working directory is the workspace root. The environment is `PATH`, `HOME` and `LANG`, plus each `--pass-env`. `pty` is `True` or `{rows, cols, term}` (default 24, 80, `xterm-256color`). `timeout` can only shorten `--exec-timeout`.

```
@t/stdout/read/{start, end?, chunk?, wait?}  → output, as a read ticket's read, but like a pipe (below)
@t/stdout/stream/{id, start, end?, ...}      → as a read ticket's stream, to @t/stdout/data/{id}
@t/stderr/...                                  the same for stderr. Not there with a pty
@t/stdin/append/{seq, data}                  → written to stdin, as a write ticket's append
@t/stdin/close                               → EOF; on a pty, the terminal's EOF character (^D)
@t/resize/{rows, cols}                       → pty only: the window size; the process gets SIGWINCH
@t/status/{wait?}                            → {running, exit_code, pty, stdout_size, stderr_size}.
                                               wait: up to that many seconds for the exit
@t/kill/{signal?}                            → signals the process group (default SIGTERM)
@t/close                                     → ends the process, drops its output
```

Output works like a file that's still being written. Only one thing differs, and it's the difference between a file and a pipe:

- A `read` waits up to `wait` seconds (default 10, at most 30) for output past `start`, then replies with what there is, like read(2) on a pipe. Each reply carries `o=<offset>`. If there's nothing, a single empty reply says `end=<offset>` for EOF or `at=<offset>` for nothing yet.
- A `stream` with no `end` runs until the process closes that stream. While the process is quiet, the server publishes an empty `at=<offset>` sample every second, so the client can tell quiet from lost.
- Each stream keeps its last 64 MiB. Offsets count every byte ever produced, and asking for older output fails with `OutputDropped`. Output stays readable after the process exits, until the ticket closes.

Plain and interactive commands use the same calls. On a pty, stdout and stderr are one stream (the terminal), stdin is always open, and the terminal echoes what's typed. Ctrl-C is the byte `\x03` on stdin: the line discipline turns it into SIGINT, as on a local terminal. `exit_code` is negative for a signal (`-2` for SIGINT), as in `subprocess`.

A process lasts as long as its ticket. When the ticket is closed, or goes `ttl` seconds without a request, the process group gets SIGHUP (on a pty) or SIGTERM, then SIGKILL 2 s later. `client.Process` heartbeats for as long as it's open.

**Every ticket**, process tickets included:

```
@t/heartbeat               → {ttl}. Counts as use, nothing else
```

Find shares with `synq topic list --type FsRead`: a share's topic is its FsRead's, without `/fs/read`.

## Mounting

`expose-synq mount TOPIC MOUNTPOINT` mounts a share with FUSE (libfuse 3, through mfusepy), until Ctrl+C or `fusermount3 -u MOUNTPOINT`. It takes the same zenoh options as `share`. It mounts read/write if discovery finds the share's FsWrite, read-only otherwise. On NixOS, point `FUSE_LIBRARY_PATH` at `libfuse3.so`.

Each FUSE call is the RPC of the same name, and an open file is a ticket: a read ticket, a write ticket, or both for `O_RDWR`. A remote error's errno reaches the program as is: excluded paths are ENOENT, read-only ones EROFS.

- Writes return once they're sent. `close(2)` and `fsync` wait for them, so a failed write shows up there at the latest, as on NFS.
- The mount sees its own writes: `stat` and reads wait for its writes in flight to that path first.
- A file truncated or replaced by someone else fails a read with EIO (the ticket's `FileChangedError`). Truncates through the same mount don't.
- There are no RPCs for `readlink`, `symlink`, `link`, `chown` or timestamps, so those fail with ENOSYS. Symlinks are listed but can't be followed, and `touch` on an existing file fails.

## Running commands

`expose-synq run --topic TOPIC COMMAND...` runs a command in a share that has `--allow-execute`, attached to your terminal like ssh, and exits with its status (128 + N if signal N killed it, 255 if the share stops answering). `--topic` defaults to `$EXPOSE_SYNQ_TOPIC`. It takes the same zenoh options as `share`, and options go before `COMMAND`: everything from it on belongs to the command.

- **On a terminal** (stdin and stdout both are, or `-t`): the command gets a pty the size of yours, and your terminal goes raw. Every keystroke goes to the remote, Ctrl-C included, and resizing your terminal resizes the remote's.
- **Otherwise** (or `-T`): stdin is forwarded until EOF (`-n` to not), stdout and stderr come back separately, and Ctrl-C sends SIGINT; a second one gives up.

Both are the same process ticket, through `client.Process`. Until SpiriSynq escapes RPC arguments, a `;` anywhere in the command breaks the call: write `sh -c` scripts with newlines instead.

## File commands

`expose-synq file VERB` is one small command per FsRead/FsWrite method, plus `pull` and `push` over tickets, for scripts and agents that would rather not mount. A remote path is `TOPIC:PATH`, as scp writes `host:path`. Without a `TOPIC:` the topic comes from `$EXPOSE_SYNQ_TOPIC`, so an agent's shell can set it once; write `:PATH` or `./PATH` for a path holding a colon. They take the same zenoh options as `share`.

```bash
export EXPOSE_SYNQ_TOPIC=host/expose/proj
expose-synq file ls src                  # list_dir: mode, size, mtime, name (--json for the raw entries)
expose-synq file stat src/app.py         # stat (--json too)
expose-synq file hash src/app.py         # hash: sha256:…  (-a for another algorithm)
expose-synq file glob '**/*.py' src      # glob: one path per line
expose-synq file grep 'def main' src     # grep: path:line:text; exit 1 if no match, 2 on error
expose-synq file pull src/app.py ./app.py   # open_read; to stdout without LOCAL; --start/--end for a byte range
expose-synq file push ./app.py src/app.py   # open_write, truncating (--append, --exclusive); - for stdin
expose-synq file edit src/app.py 'old' 'new'   # edit_file; --old-file/--new-file for multi-line text
expose-synq file mkdir -p a/b            # also rm [-r], mv, chmod 644 PATH
```

Each does what its method does and no more: `push` writes in place, with no temp file and rename (push, then `mv`, for that), and `mv` works only within one share.

## Limitation: no access control

This prototype has **no access control**. expose-synq doesn't check who's calling, and it doesn't configure or rely on Zenoh `access_control`. Anyone who can reach the node over Zenoh can call every capability you granted, including `--allow-execute`. Only run it on networks you trust. The default config has no connectivity, while `--public` makes it reachable by every Zenoh peer on the network.

Tickets (`<root>/tickets/@<secret>`) can't be found by wildcard queries or subscriptions, because Zenoh wildcards never match an `@` segment. But a ticket is a bearer secret: anyone who learns its key can use it.

Who can reach the node is zenoh's job: TLS and `transport/auth` in its config (see below). Within the workspace, `PathPolicy` keeps paths inside the root (no `..`, no symlinks along the way) and applies the excludes and read-only paths.

## Zenoh configuration

By default expose-synq doesn't listen, connect or scout at all. You open up connectivity, and secure it, through zenoh's own config: TLS, `transport/auth` and `access_control`. Each layer below overrides the ones above it:

1. the locked-down base; `--public` adds zenoh's normal multicast and gossip scouting on top. It opens no listener: peers are reached by connecting out to the listeners they advertise.
2. a JSON5 config file: `--zenoh-config FILE`, `EXPOSE_SYNQ_ZENOH_CONFIG` or `ZENOH_CONFIG`, else `~/.config/expose-synq/zenoh.json5` if `expose-synq setup` wrote one; then the config each server handed `share` on [enrollment](#enrolling-with-a-server)
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

### `expose-synq setup`

An interactive wizard that writes `~/.config/expose-synq/zenoh.json5` (or `-o FILE`). It asks for:

- **Interfaces** to listen on, ZeroTier ones first with their network names, plus multicast scouting on the first one and any peers to connect to. Port 0 lets several expose-synq processes run on one machine; peers find each other by scouting. Network names come from the ZeroTier service's local API, with the token from `~/.zeroTierOneAuthToken` or `ZEROTIER_AUTH_TOKEN`. Without one, the wizard offers `sudo zerotier-cli -j listnetworks`, or lists the `zt*` devices without names.
- **Authentication**, in the forms zenoh can do from a config file: none (trust the network, e.g. ZeroTier membership), a shared username and password (`transport/auth/usrpwd`), TLS, mTLS, or mTLS plus a password. TLS links can be `tls` or `quic`. It can create a CA and sign this node's certificate (EC P-256, with the hostname and the chosen IPs as names), sign one with an existing CA, or use certificate files you already have.

Keys, the password file and the config are written mode 0600 next to the config. On the next machine, copy `tls/ca.crt` and `tls/ca.key` over and choose `existing` at the certificates step; for a password, enter the same one.

zenoh's `transport/auth/pubkey` isn't offered. In zenoh 1.10, `known_keys_file` is read into the config but not into the authenticator, so every key is rejected as unauthorized.

### Enrolling with a server

For `share`, a `--connect` that is a host or a URL instead of a zenoh endpoint names a server to ask for access:

```bash
sudo uvx expose-synq share / -x --no-sandbox -c hermes.example.com
```

```
Asking https://hermes.example.com for access as boxa.
  code: 7K2Q-M9XD   approve it there if the code matches
Waiting for approval (Ctrl+C to stop; running again resumes the request)
```

The first time, `share` makes an Ed25519 key, sends the server its hostname (the node's base topic) and public key, and prints a code: a fingerprint of the key, which the server's approval page shows too. A person approves it there if the two match. `share` polls until the request is approved, denied or expires. Ctrl+C keeps the request, and running again picks it up.

The approval carries a zenoh config layer (endpoints, certificates) and the files it refers to. They're saved, mode 0600, under `~/.config/expose-synq/servers/<host>/` and used until the approval expires, then `share` asks again with the same key. The server's endpoints are added to any other `--connect` endpoints. The protocol is in the docstring of `synq_expose/enroll.py`.

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

- `cli.py`: the Typer app (`share`, `mount`, `run` and `setup`); validates flags into an `ExposeConfig` and serves
- `mount.py`: `SynqFS`, the FUSE operations over the RPCs and tickets
- `run.py`: `run_command`, a remote process attached to local fds, on a pty or pipes
- `config.py`: `ExposeConfig` and friends, plus the layered zenoh config and `reopen_session()`
- `wizard.py`: `expose-synq setup`; `build_config()` turns its answers into a zenoh config, and it makes the certificates
- `interfaces.py`: network interfaces (`ip -j addr`) and ZeroTier networks from the local service
- `workspace.py`: `build_objects()`, which creates one object per granted capability, and `discover()`, which finds them from the client side
- `tickets.py`: `TicketRegistry`, which creates, serves and revokes tickets
- `fs.py`: `FsRead`, `FsWrite`, the file read/write streams, and `ByteStream`, the read/stream routes shared with process output
- `shell.py`: `Shell.spawn`, and `Process`: a process's output as pipe-like read streams, its stdin as appends
- `policy.py`: path confinement, excludes, read-only globs
- `guard.py`: `--confirm` and the audit log
- `sandbox.py`: bubblewrap argv
- `client.py`: the agent side of the ticket streams
- `testing.py`: the loopback test network
