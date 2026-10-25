# Pinned rsync verification-failure oracle

Reference source: rsync 3.4.1 official archive, SHA-256
`2924bcb3a1ed8b551fc101f740b9f0fe0a202b115027647cf69850d65fd88c52`.
The ordinary conformance oracle remains unmodified. A **separate test-only copy**
was built with optional xxhash/zstd/lz4/OpenSSL/ACL/xattr disabled.

In that copy's `receiver.c`, immediately after
`read_buf(f_in, sender_file_sum, xfer_sum_len);`, insert:

```c
/* Test-only: leave the actual retry state machine unchanged. */
{
    static int injected = 0;
    const char *mode = getenv("PEERSYNC_TEST_FAIL_CHECKSUM");
    if (fd != -1 && mode && (mode[0] == '2' || injected++ == 0))
        sender_file_sum[0] ^= 1;
}
```

Run one 8192-byte old/new file differing in one byte, with
`-rc --no-whole-file --checksum-choice=md4 --debug=DELTASUM2`.
Local rsync normally defaults to whole-file transfer, so `--no-whole-file` matters.

Observed:

| Fault | Attempts / s2length | Exit | Destination |
| --- | --- | --- | --- |
| Env `...=1` (first verification fails) | 2 / 2 then 16 bytes | 0 | New content |
| Env `...=2` (both fail) | 2 / 2 then 16 bytes | 23 | Original content |

Both attempts still generated block signatures and delta matches. They did not
silently switch to whole-file transfer. The second failure reported “update
discarded”. This tests one deterministic failure branch, not every rsync option.

The peersync `tests/retry.rs` oracle uses a custom DeltaEngine to emit a **valid
delta for deliberately wrong content**, causing final BLAKE3 mismatch. Its tests
assert two attempts, 16-byte checksum on redo, new content on one injected failure,
and original content retained after two. This is the matching supported contract;
exit numbers, checksum algorithms and exact delta bytes remain different.
