# Initial behavior contract

Reference: rsync **3.4.1**. We implement a selected behavior subset, not its wire
protocol, exact itemized output, option grammar, delta byte-for-byte decisions, or
exit codes. Tests compare actual trees, bytes, supported modes/times and error
effects. No full compatibility claim is made.

## Copy and metadata

- Copy source directory contents, including empty directories and the root's
  selected metadata. Default quick-check compares regular-file length and mtime
  in whole seconds; checksum mode uses BLAKE3.
- Existing target regular-file permissions survive updates by default. New files
  and directories use source ordinary permissions subject to receiver umask.
  Preserve-permissions explicitly applies Unix modes; preserve-time applies
  seconds/nanoseconds. Directory metadata is applied after descendants.
- Temporary regular files are exclusively created in their destination parent.
  Verify reconstructed length and BLAKE3 before replacement; preserve the old file
  on verification failure. No claim of filesystem crash durability is made.
- Preserve symlinks only when requested. Source links are not followed. A source
  real directory replaces an existing target directory symlink without traversing
  that link. Absolute/outside source link text is copied as text, not read through.
- Empty old directories may be replaced by a file/link. With delete enabled,
  removable contents can be removed to make way; protected descendants still
  prevent removal. Without delete, a nonempty old directory produces an error.
- Unix filenames are byte-preserving in the protocol. Display text is not used
  for path identity. Cross-platform representations can explicitly fail.

## Filters

`FilterMode::{None, Glob(rules), GitIgnore}` are mutually exclusive; the CLI
rejects combining `.gitignore` and explicit rules before opening a connection.

Glob mode uses globset with literal separators, case sensitivity, and backslash
escaping. Patterns containing `/` match root-relative paths; otherwise they match
basenames. First matching rule wins, unmatched means included. Excluded directories
are pruned. Include does not force transmission of unchanged content. These are
declared matching rules, **not full rsync filter syntax**.

GitIgnore loads only root-local and nested `.gitignore` without requiring a Git
repository. It uses ignore's matching, negation and pruning behavior. No global,
parent, `.ignore` or `.git/info/exclude` rules are loaded. Hidden attributes do not
automatically exclude a file. Source-side rule text is transmitted for deletion
protection, rather than trusting a possibly different target `.gitignore`.
At most 1024 explicit rules or .gitignore files and 64 KiB aggregate pattern text
are supported; explicit patterns are at most 4096 bytes. Invalid/excessive rules
fail explicitly. Files excluded by source GitIgnore parents are not searched for
deeper rules.

## Deletion and failures

Delete is opt-in, not a second authorization layer. The receiver deletes extra
immediate children before updating that directory's children (delete-during), not
a fixed after-transfer pass. Target-only subtrees are removed without following
links; excluded descendants prevent their parent from being removed.

The scanner currently buffers a bounded manifest, but replays per-directory
lists with parent files before subdirectory lists. Scan errors are attached to
their affected directory. Once encountered, subsequent deletions are suppressed;
earlier ancestor deletions are not undone. The pinned tests include an unreadable
subdirectory to exercise this distinction from globally suppressing all deletion.
This is not a claim to reproduce every rsync scheduling outcome under concurrent
filesystem changes. Unknown/unlocatable errors suppress from the root.

Errors can leave committed changes. Recoverable verification failures redo once
using a full 16-byte signature checksum, then fail without replacing the file.
The retry remains a delta when the first attempt was a delta; it is not silently
converted into a whole-file fallback. Malformed/truncated transport is fatal;
we cannot continue on an uncertain frame boundary. Some filesystem errors are
also fail-fast rather than reproducing rsync's complete error-continuation policy.
Reports mark partial/uncertain effects; callers must reconcile, not blindly replay.

## Resource and cancellation contract

One active task per Syncer; a competing task fails fast. Separate Syncers use
independent budgets. Control frames, data frames, manifest entries/estimated bytes,
signatures, reconstructed output and report issues are bounded. Defaults are
256 KiB control, 64 KiB data, 100000 entries, 128 MiB working budget, 100 issues.
These are conservative working-data limits, **not a hard process RSS or disk quota**.
Custom DeltaEngine implementations must honor their context's limits.

fast_rsync requires buffered inputs. Files and bases within the configured delta
threshold use its legacy MD4 block signatures only for matching, never as trusted
content authentication; reconstructed files are independently checked with BLAKE3.
Files and bases within the configured delta
threshold (working budget / 16) may use deltas. Otherwise a full-file stream is
selected *before transfer*, with chunked hashing into a destination temporary file.
This preserves large-file support without claiming streaming delta computation.
Manifest/error retention and filter compilation remain bounded in-memory work.
Local source ignore traversal uses the upstream ignore implementation, not an
OS-enforced memory sandbox. Disk capacity/file counts still require host policy.

File/delta blocking work is dispatched to workers; progress callbacks must be fast.
Cancellation waits for in-progress non-preemptible work to finish at cooperative
boundaries, stops subsequent changes and cleans temporary paths. It does not roll
back completed replacements. Directly dropping a future cannot provide the same
cleanup-wait guarantee. The host owns stream reset/stop and connection shutdown.

## Remaining release gates

The complete cross-platform matrix, external security review, relay/network-fault
matrix, broad long-running randomized filesystem differential suite, and exhaustive
rsync redo/error-path comparison remain future acceptance work. The initial local
Linux implementation is not evidence that these gates passed.
