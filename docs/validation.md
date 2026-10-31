# Initial Linux validation record

## Ripgrep filtering migration (local validation)

- Rust 1.95.0 / ignore 0.4.33; oracle ripgrep 15.2.0.
- Workspace: 38 tests plus 1 compiled doctest pass; three external-oracle tests
  are ignored by default. Strict workspace Clippy and formatting pass.
- `PEERSYNC_RG=/path/to/rg cargo test -p peersync-cli --test processes
  filter_selection_matches_pinned_ripgrep --locked -- --ignored` passes all six
  option combinations with actual CLI peers and isolated source Git/HOME config.
- Both pinned rsync 3.4.1 conformance tests pass with filtering explicitly disabled;
  they continue checking transfer behavior, not the new filtering contract.
- Added source-rule deletion protection independent of target rules, positive
  globs/last-match behavior, protected directory replacement, malformed/cancelled
  filter replies, traversal rejection and old-version/filter-shape rejection.
- Both send/receive futures have a compile-time `Send` check; deep candidate
  ancestor-cache expansion has an explicit estimated-budget regression test.
- Source-side candidate evaluation avoids transmitting private ignore contents.
  Candidate paths/types are sent to the source when delete is enabled.
- The checks above were rerun before local integration into `main`; no push was
  made. The following sections retain historical evidence for earlier
  implementation stages, not current publication status or filtering defaults.

This is evidence for the initial implementation, not production certification or
full rsync compatibility. No Naco code, Cargo configuration or dependencies were
changed. No remote repository was created, no commit pushed, no crate published.

## Build and tests

- Rust/Cargo **1.95.0**, Linux, edition 2024: workspace builds/tests pass.
- `cargo test --workspace --locked`: **25 tests + 1 compiled doctest pass** after
  the EndpointId-based CLI refactor; the
  two external-oracle tests are intentionally ignored in that default command.
- `PEERSYNC_RSYNC=.../rsync-3.4.1 cargo test -p peersync --test conformance -- --ignored`:
  **2 tests pass**, covering multiple preserve/delete combinations and nested
  scan-error deletion timing plus directory-to-file replacement.
- fmt check passes. Clippy `--workspace --all-targets --locked -- -D warnings`
  passes using the matching Nix Rust/Cargo/Clippy **1.99** toolchain in a separate
  target directory. This is additional lint coverage; the build baseline remains
  the tested 1.95 compiler. Hosted CI has been defined but not run remotely.
- `cargo package -p peersync --allow-dirty --locked` packages and verifies the
  library locally. Homepage/repository/documentation metadata awaits a real remote;
  packaging is not publication or a reservation of the crate name.
- `cargo tree -p peersync --edges normal --locked`: no Iroh dependency.

Tests include randomized delta roundtrips; malformed signatures and output budgets;
valid-but-wrong delta reconstruction followed by 16-byte checksum redo; unchanged
old file after two failures; small-capacity duplex framing/backpressure; cancellation;
path traversal rejection before root creation; oversized frame rejection; partial
stream temporary-file cleanup; same-instance concurrency failing without deadlock;
quick-check/checksum, hidden files, empty directories, opt-in delete, dry-run,
glob override protection, nested GitIgnore in a repository, non-UTF8 Unix names,
target directory symlink replacement and dry-run safety. Two actual CLI processes
sync over local Iroh, reject an unauthorized identity, and refuse key overwrites.

The separate instrumented rsync oracle verified one failure retries with checksum
length 2→16 and succeeds; two failures stop and retain the old file. See
[redo oracle](redo-oracle.md). This is not a complete rsync retry-option matrix.

## CLI connection refactor

Removed connection ticket file creation/parsing. Argument tests cover EndpointId-only
default setup, optional address hints, repeated IPv4/IPv6 hints, required addresses
in direct sender mode, invalid addresses, removed `--ticket`, and receiver startup
without a ticket. Actual two-process tests consume the receiver's flushed readiness
JSON line, then connect using `--peer` plus `--addr`; they verify final reports,
reject unauthorized senders, and reject a wrong receiver identity even when the
address hint points to the real receiver. Key creation remains create-only.
Default public discovery/relay is not exercised by these isolated direct tests.
The core library and synchronization wire protocol are unchanged by this refactor.

## Resource probes

Release `resource_probe` creates inputs chunkwise, runs both roles over bounded
duplex, and verifies through the normal protocol. GNU time reports whole-process
RSS, including both roles/runtime/harness. These are local cached-filesystem runs,
not network throughput benchmarks or guaranteed maximums.

| File size | Configured budget per role | Selection | Max RSS | Sync elapsed |
| --- | --- | --- | --- | --- |
| 128 MiB | 1 MiB | Whole-file stream | 2632 KiB | 149 ms |
| 1 MiB | 32 MiB | Buffered delta (unrelated basis) | 8360 KiB | 4 ms |

Commands:

```sh
cargo build -p peersync --example resource_probe --release --locked
time -v target/release/examples/resource_probe 134217728 1048576
time -v target/release/examples/resource_probe 1048576 33554432
```

## Dependencies and review

`cargo audit` 0.22.2 scanned 399 locked dependencies against 1352 loaded RustSec
advisories. It reported no known vulnerability findings and **one unmaintained
warning**, `paste 1.0.15` / RUSTSEC-2024-0436, through
`netlink-packet-core → netwatch → iroh → peersync-cli`. This is not in the core
library tree. Do not hide the warning or equate audit success with a security proof.

Cargo metadata enumerated license expressions for every package (none missing).
This is an inventory, not legal review. Review notice/redistribution obligations
and project ownership before release. The initial project license is MIT.

Manual implementation checks addressed length-before-allocation, private wire
types, signature validation before indexing, independent BLAKE3, bounded error
retention, filter-source exclusivity, no symlink-following for target ancestry,
local root selection, create-only identities, caller-owned stream lifecycle, and
explicit peer identity authorization. Hostile local TOCTOU and filesystem snapshots
are explicitly outside the first-version guarantee.

## Still not established

- macOS/Windows acceptance, public relay behavior and broad real-network failures.
- Hard process-memory/disk quotas, exhaustive decoder fuzzing, independent security
  review, or long-running randomized filesystem/delta workload benchmarks.
- Full rsync error-continuation, all concurrent-change outcomes, filters, options,
  output/exit compatibility, or byte-identical deltas.
- Hosted CI execution, name reservation, publishing and Naco integration.

Read [behavior.md](behavior.md) for the supported subset and explicit fail-fast
boundaries. The above limitations are not silently treated as completed gates.
