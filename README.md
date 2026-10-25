# peersync

An embeddable Rust library for one-way, rsync-style directory synchronization
over reliable ordered byte streams, with a thin Iroh CLI.

**Initial Linux implementation; not a complete rsync replacement or a security
sandbox.** Core package `peersync` has no Iroh dependency. Package `peersync-cli`
provides the `peersync` command. This workspace is independent of Naco.

## Build and test

Rust 1.95 or newer; edition 2024. The workspace lockfile pins the tested dependency
resolution. Runtime operation launches no rsync, SSH, or other subprocess.

```sh
cargo build --workspace --locked
cargo test --workspace --locked
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
```

On Nix, use a matching toolchain (mixing a 1.99 clippy-driver with 1.95 rustc does
not work):

```sh
nix shell nixpkgs#rustc nixpkgs#cargo nixpkgs#rustfmt nixpkgs#clippy
```

## Library

See the crate's compiled rustdoc example for paired in-memory streams:

```rust,ignore
let syncer = peersync::Syncer::new();
let mut options = peersync::SyncOptions::default();
options.preserve_mtime = true;
let cancel = peersync::CancellationToken::new();
let mut context = peersync::SyncContext::new(cancel.clone());

let result = syncer.send(
    &mut reader, &mut writer, source_path, &options, &mut context,
).await;
```

The other endpoint calls `receive(reader, writer, destination_path, context)`.
Both roles need both halves. Roots are local `&Path` arguments; the peer never
selects the receiver's absolute root. Roots always mean **directory contents**,
regardless of a trailing slash. Relative paths resolve when the operation starts.

Use separate Syncers for independent concurrent operations, including two roles
in the same process. A Syncer permits one active task; concurrent use returns a
resource error immediately, rather than waiting and potentially deadlocking.
Repeated sequential use is supported. It never closes an underlying connection.

`SyncContext` optionally accepts a fast synchronous `FnMut(&SyncEvent) + Send`
observer. Callbacks run serially on the coordinator, must not block, and should
use `try_send` if forwarding to an application-owned queue. Final reports do not
depend on receiving every progress event. Library tracing does not install a
global subscriber.

On failure, `SyncFailure` preserves a boxed `SyncReport` and concrete `SyncError`.
Boxing keeps the Result error variant small. Completed changes are not rolled
back. Lost confirmation may make remote effects uncertain; never blindly replay.
Cancel the token **and await completion** for cooperative cleanup. Directly
dropping the future cannot await blocking workers. Discard failed/cancelled streams.

## CLI: one-shot authorized receiver

Create identities on the respective machines:

```sh
peersync keygen --out sender.key    # prints sender EndpointId
peersync keygen --out receiver.key  # prints receiver EndpointId
```

Key creation is create-only, with Unix mode 0600. Keys contain 32 raw private
bytes, not JSON or hex. Do not distribute private keys.

Start the receiver, authorizing the printed sender identity:

```sh
peersync receive ./destination --key receiver.key \
  --allow-peer SENDER_ENDPOINT_ID
```

The receiver prints its EndpointId and current direct IP/UDP addresses. Obtain
the receiver's EndpointId through a **trusted channel**: that identity determines
whom the sender authenticates, regardless of how network addresses are discovered.
There is no connection-information file to generate, copy or refresh.

```sh
peersync send ./source --key sender.key --peer RECEIVER_ENDPOINT_ID --times
```

Without explicit addresses, the default Iroh N0 preset discovers addresses from
the EndpointId and enables public relays. It does not require every data transfer
to traverse a relay. Public discovery/relay operation has not been validated by
the local process tests.

Use `--direct` on both sides for local/LAN testing without public relays or address
lookup. A direct **sender** must provide at least one `--addr IP:PORT`; repeat it
for multiple address hints. Explicit hints are also accepted in default mode.
Addresses never replace identity verification.

For example, in two terminals on the same machine:

```sh
# Receiver: print identity and listen at a known local address.
peersync receive ./destination --key receiver.key \
  --allow-peer SENDER_ENDPOINT_ID --direct --bind 127.0.0.1:12345

# Sender: pin that identity and provide the reachable UDP address.
peersync send ./source --key sender.key --peer RECEIVER_ENDPOINT_ID \
  --direct --addr 127.0.0.1:12345 --times
```

`--bind IP:PORT` controls the local bind address; port 0 requests an ephemeral port,
which the receiver prints. For different machines use an appropriately reachable
address, not the receiver's loopback address. `--direct` alone does not restrict
the listener to localhost. Repeated runs reuse identity keys and require restarting
the one-shot receiver, but no ticket files. The old `--ticket` option is removed.

The receiver accepts one authorized operation then exits. `--allow-peer` grants
that sender permission to modify the selected destination, **including deletion**;
there is no second `allow_delete` layer. Library hosts must perform equivalent
operation authorization themselves.

Sender options:

| Option | Meaning |
| --- | --- |
| `--checksum` | Compare content rather than size/mtime |
| `--whole-file` | Disable block deltas |
| `--delete` | Remove unprotected extra target entries, reference ordinary rsync 3.4.1 `--delete` |
| `--dry-run` | Plan without modifying/creating the destination |
| `--times`, `--perms`, `--links` | Preserve supported mtimes, Unix modes, or symlinks |
| `--include PATTERN`, `--exclude PATTERN` | Ordered globset rules; first match wins |
| `--gitignore` | Root-local/nested `.gitignore` only; conflicts with explicit rules |

Defaults do not read ignore files, skip hidden files, exclude `.git/`, follow source
symlinks, preserve mtimes/modes, or delete extra target entries. Source symlinks and
special files not selected for copying are skipped and retain their manifest names
so a same-name target is not accidentally removed by `--delete`.

`--json` emits newline-delimited JSON: a receiver emits a flushed `type: "ready"`
record containing `endpoint_id` and `addresses` before waiting for a sender, and
both roles emit a `type: "report"` record when the library operation returns.
Senders do not emit a readiness record. `keygen` continues to print just its ID.
Logs and argument/setup errors go to stderr. `--timeout SECONDS` defaults to 300;
zero disables the timeout. Ctrl+C/timeout cooperate with cleanup and exit 130;
other errors exit 1, argument errors 2, success 0. Rsync exit-code compatibility is
not promised. `RUST_LOG=debug` enables structured progress logs.

## Compatibility and safety boundaries

See [behavior contract](docs/behavior.md), [protocol](docs/protocol.md), and
[validation evidence](docs/validation.md). In particular:

- Per-file temporary replacement, not a directory transaction or durable fsync.
- No concurrent-edit conflict detection, stable snapshot, resumable transfer,
  bidirectional synchronization, watcher, owner/group, ACL/xattr, special-file
  creation, hard-link topology, sparse-layout preservation, or rsync wire protocol.
- Linux is the tested platform. macOS/Windows support is not claimed.
- Local roots must be trusted/dedicated. Relative path and symlink checks do not
  defeat hostile concurrent local directory replacement (TOCTOU).
- The existing rsync behavioral probes are not proof of complete compatibility;
  the supported subset and failure-boundary limitations are explicit in the docs.

## Development oracle and resource probes

Only the opt-in tests invoke rsync. Supply the separately built pinned binary:

```sh
PEERSYNC_RSYNC=/path/to/rsync-3.4.1 \
  cargo test -p peersync --test conformance -- --ignored

cargo build -p peersync --release --example resource_probe --locked
time target/release/examples/resource_probe 134217728 1048576
```

Do not publish until the package names, licensing ownership, platform matrix and
release checklist have been reviewed by the project owner.
