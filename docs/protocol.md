# Protocol v2 (private implementation contract)

An operation exclusively borrows one reliable ordered read/write stream pair.
Authentication, peer and operation authorization, endpoint discovery and transport
shutdown belong to the host. Core has no Iroh dependency.

Frames: `[u8 kind][u32 big-endian payload length][payload]`.
Kind 1 is bounded JSON control, kind 2 bounded raw data (no Base64). Reject length
before allocating payload. JSON parsing rejects unknown message variants/fields;
ordinary serde_json recursion limits remain enabled. Empty raw frames during a
nonempty payload and unexpected message/state transitions are errors.

1. Sender `Hello`: protocol version, signature/delta format identifiers and format
   version, SyncOptions. Receiver validates and responds `Ready`. Format mismatch
   fails before filesystem writes. No speculative multibackend registry.
2. Sender emits individual `Entry`, `ScanIssue`, then `ManifestEnd`.
   Each Entry has a component-array byte path, kind, size, time, mode and optional
   comparison digest/link text. The manifest begins with the root Directory.
   Reject escaping paths, duplicate entries and missing/non-directory parents
   before creating the destination. ScanIssue names affected directory lists.
3. Receiver processes lists, deletion and local quick-checks. For files requiring
   content it emits `Request {index, hash_bytes, signature_len}` plus bounded raw
   signature frames. Zero signature_len selects whole-file transfer. The sender
   only serves validated manifest indices, with at most two requests per file.
   Before deleting, it sends `FilterQuery { candidates: [{path,is_dir}] }` and
   waits for a same-length `FilterResult { included: [bool] }`. These are legal
   only when delete was approved. Batches are at most 256 entries and fit a
   control frame; operation-wide candidate counts/estimated bytes are bounded.
   The source uses its own ignore context, validates relative paths and refuses
   symlink-ancestor traversal. The receiver descends only into approved target
   directories and plans before deleting within each visited directory. It never
   treats unknown entries or an absent/malformed decision as approval.
4. Delta content is `Content {bytes,file_len,digest,delta:true}` then exactly bytes
   raw payload. Decode against the retained basis, limit output to file_len, verify
   length and BLAKE3, then replace. Failure regenerates the signature with 16-byte
   checksum and repeats once; no implicit delta-to-whole retry.
5. Whole-file content uses `Content {bytes:file_len,file_len,digest:"",delta:false}`,
   chunked raw frames, then `ContentEnd {digest}`. Receiver streams to a temporary
   file and verifies the final digest before commit. Missing end/digest is failure,
   never successful EOF. Sender checks truncated/growing source lengths but offers
   no snapshot guarantee under same-length concurrent changes.
6. Receiver acknowledges successful file commits with `ItemResult`. A per-request
   source read failure uses `Failed` and is recorded before continuing the request
   loop. Other unexpected Failed states terminate the task.
7. Receiver sends `Done` counters; sender returns `Finish`. Reports distinguish
   confirmed completion from unknown effects. Finite handshakes cannot eliminate
   all lost-ack uncertainty. EOF is not a substitute for successful completion.

The receiver never
accepts a different option set when the host passes `expected_options` to
`Syncer::receive`: the Hello must match before any destination mutation. Passing
`None` explicitly permits the authenticated sender to choose options. This is a
host-approval contract. Version 2 changes the filter options and replaces rule
text transmission with candidate queries; version 1 is rejected.

Cancellation or framing failure invalidates this operation's streams. Core never
closes a host's shared connection. The CLI owns its one-shot connection. After
reading `Finish`, only the receiver finishes its send stream, waits for transport
acknowledgement, and closes the connection with code zero. The sender keeps its
streams alive and waits for that normal remote close; it does not independently
close after its own transport ACK. Symmetric close would race the other side's
last ACK. Completion is bounded by a five-second timeout and cancellation;
abnormal close remains an uncertain failure. The CLI reset/stops streams on
sync failure.

Protocol structs are private; public types are not a general serialization
compatibility guarantee. Before evolving a message, change negotiation and test
incompatibility explicitly. Iroh ALPN is `peersync/2` in the CLI only.

CLI connection setup is separate from this protocol. Senders specify `--peer`
(EndpointId) plus optional repeatable `--addr` hints. Default N0 configuration
provides address lookup; `--direct` disables it and requires an address hint on
the sender. The receiver prints its identity/addresses and still checks
`--allow-peer`. There is no ticket-file transport format. JSON readiness/report
lines on stdout are CLI output, not network control frames.
