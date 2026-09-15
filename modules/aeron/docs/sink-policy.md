# Durable sink used by the handoff example

`DurableSink` is a small local append journal, independent of Aeron and its Archive
directories. It is an example bridge for file I/O, checksums, locking and forcing.
Flix owns record coordinates, polling, acknowledgement publication, required sink
obligations, replay pins, storage caps and reclamation decisions.

## Commit and recovery contract

`open(path)` obtains an exclusive process lock on `path/records.nsk`. New
directories and their parent directory entries are forced. The journal is forced
and its directory is forced before open succeeds. An unsupported directory-force
operation fails open; there is no silent weaker policy.

`commit(sourceKey, start, end, bytes)` stores one checksummed record containing the
source identity, a record identity, the half-open byte range `[start,end)`, and the
application bytes. The end coordinate is that record's checkpoint candidate. The
whole committed set is the retained checkpoint state; there is no independently
updated scalar that could get ahead of its source mapping or bytes. The method
returns its locator only after `FileChannel.force(true)` completes. Java's force
operation requests persistence through the local filesystem; this is not a claim
about arbitrary device caches, broken storage hardware or every power-loss mode.

The journal record is `magic:u32`, `bodyLength:u32`, `~bodyLength:u32`, `body`, `CRC32C:u32`,
`commitMarker:u32`, using Java's big-endian encoding. The body contains length-prefixed
UTF-8 source and application identities, `start:i64`, `end:i64`, and length-prefixed
opaque bytes. Payloads are bounded to approximately 64 MiB; keys to 16 KiB. The
length complement prevents a corrupt length from disguising a complete record as
an incomplete tail. These checks detect accidental corruption, not hostile modification.

After restart, a complete checksum-valid record is retained, including one whose
caller died before observing success. An incomplete final record is copied to a
forced `incomplete-tail-*.bin` file, its directory entry is forced, and only then is
the incomplete suffix truncated and the journal forced. The raw suffix remains
inspectable. A fully present record with an invalid checksum/header/commit marker
fails open, preserving the journal for diagnosis; it is not silently forgotten.
I/O failure during commit makes the current handle unusable until close/reopen,
because commit outcome may be unknown. Exact retries after reopening are safe.

This is a lab sink: recovery indexes all records and payload bytes in memory; it
has no compaction or multi-writer feature. The application must choose a storage
cap and stop/backpressure before exceeding it. Indefinite sink failure cannot
promise bounded Archive storage.

## Coordinates, identities and acknowledgements

`sourceKey` must name an Archive incarnation plus recording ID. A numeric
recording ID alone is insufficient. The convenience `commit` uses the start
coordinate as its source-local record identity. `commitRecord` accepts an explicit
application identity when one is available. An exact identity/range/bytes retry
returns the original locator; conflicting reuse and overlapping ranges fail.
Bytes stay opaque and are copied on commit and lookup.

`persistedThrough(sourceKey, trustedBase)` walks adjacent committed ranges from
the supplied base. It never takes the maximum observed position. For example,
`[0,64)` and `[128,192)` yield `64`; committing `[64,128)` yields `192`. The base
must be the agreed initial recording/application boundary, or a previously
established durable checkpoint. Supplying an arbitrary base is not evidence that
earlier bytes are persisted. A base inside an existing application range is
rejected. Range length is not required to equal payload length: recording positions
include Aeron headers, alignment, fragmentation and potentially term padding.
The Flix caller must establish that all bytes of a covered range are satisfied;
the sink never invents coverage over a hole or guesses frame boundaries.

The Flix example publishes a persisted-through notification only after commit
returns. A crash between commit and notification leaves enough retained state to
reconstruct and reemit the notification after open. Notification receipt can then
be duplicated: consumers must use the source incarnation and monotonic contiguous
coordinate idempotently. A waiter must register its notification interest and
check retained state before blocking, and check again after wakeup; a late waiter
can immediately complete from `persistedThrough`. This bridge does not own an
Aeron poller or start notification threads.

Locators are local `file:` URIs with journal offsets. They resolve through
`lookupLocator` after reopening the same sink path, independently of Archive
retention. Source-to-locator mappings and bytes remain outside the stream that
will be purged. Moving the sink directory changes the URI; remote or relocatable
locators and journal compaction are outside this lab contract.

Received, Archive-recorded, downstream-persisted, and consumer-applied remain
separate observations. Neither a successful Aeron offer nor an Archive progress
counter is a downstream sink commit. This sink's force policy does not rename any
Archive progress coordinate as `fsyncComplete`.

## Bounded reliability checks

`nema.aeron.SinkChecks` runs without an Aeron dependency. It verifies holes,
incarnation separation, exact retry, conflicting identity/range reuse, owned
bytes, exclusive writer ownership, closed handles, twenty reopen cycles,
quarantined torn payload/header suffixes, complete corruption failure, stable
locators, and a subprocess halted with exit code 73 immediately after a forced
commit and before notification. Reopen reconstructs the checkpoint and bytes,
and retry does not append a duplicate. Tests retain their runtime evidence in a
uniquely allocated `.nema/sink-checks-*` directory. This is process-crash and
specific torn-write evidence, not proof against every storage failure.
