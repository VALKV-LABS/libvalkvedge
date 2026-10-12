# Library components

How the library is put together: the crates, what each one owns, and how a read, a
write and a failover move through them.

For the bytes that end up in the object store, see [format.md](format.md). To run any
of this yourself, see [testing-locally.md](testing-locally.md).

## The idea in one paragraph

A database is an append-only log of batches, kept in an object store. One process at a
time, the *writer*, may append to it. Every process that uses the database, writer or
not, keeps a full local copy in an embedded LSM-tree and answers reads from that copy.
Replicas bring their copy up to date by reading the log. When the log gets long the
writer writes a snapshot and starts a new *generation*, so a new replica never has to
replay history.

## Crates

```mermaid
flowchart TB
    APP["Your application"]
    CLI["valkvedge-cli<br/>inspect a local database"]

    subgraph CORE["valkvedge"]
        API["ValkvEdge, Database, Batch<br/>public API"]
        MGR["Database manager<br/>bounded open handles, eviction"]
        LOCAL["LocalStore<br/>data, outbox, overlay"]
        CRYPTO["Cipher<br/>value encryption"]
    end

    subgraph SYNC["valkvedge-sync"]
        PULL["pull<br/>frames or snapshot into a state"]
        WRITER["Writer<br/>commit, push, compact"]
        STATE["StateStore trait"]
    end

    subgraph LOG["valkvedge-log"]
        LW["LogWriter<br/>append, fencing, snapshots"]
        LEASE["Node heartbeat and lease"]
        FMT["Formats<br/>frame, segment, snapshot, manifest"]
        STORE["Store<br/>create-only and compare-and-swap writes"]
    end

    FJALL[("Fjall<br/>local LSM-tree")]
    OBJ[("Object store<br/>S3")]

    APP --> API
    CLI --> API
    API --> MGR --> LOCAL
    API --> CRYPTO
    API --> WRITER
    API --> PULL
    LOCAL -. implements .-> STATE
    WRITER --> STATE
    PULL --> STATE
    WRITER --> LW
    PULL --> FMT
    LW --> LEASE
    LW --> FMT
    LEASE --> STORE
    FMT --> STORE
    LOCAL --> FJALL
    STORE --> OBJ
```

Dependencies point one way: `valkvedge` → `valkvedge-sync` → `valkvedge-log`.

| Crate | Owns | Does not know about |
|---|---|---|
| `valkvedge-log` | The object formats and their checksums. Conditional writes. Who owns a database (leases) and how a previous owner is locked out (fencing). | What the frames mean. Any local storage. |
| `valkvedge-sync` | Turning a log into a state (`pull`). Turning writes into a log (`Writer`): ordering, conflict resolution, compaction. | Fjall. It works against the `StateStore` trait. |
| `valkvedge` | The API. The local store on Fjall. Which databases are open. The outbox. Encryption. | Object keys and lease mechanics. |
| `valkvedge-cli` | Reading a local database from the command line. | — |

`StateStore` is the seam between replication and storage. `valkvedge-sync` ships an
in-memory implementation (`MemState`) so its tests need no disk; `valkvedge` implements
it on Fjall.

## What is stored where

```mermaid
flowchart LR
    subgraph DEVICE["One database on local disk: {root}/{database}/"]
        DATA["data<br/>key → value, timestamp, deleted flag"]
        META["meta<br/>log position, device id, key check"]
        DEV["devices<br/>device → highest applied outbox entry"]
        OUT["outbox<br/>buffered batches not yet confirmed"]
        OVER["overlay<br/>latest pending write per key"]
    end
    subgraph BUCKET["The same database in the object store: {prefix}/{database}/"]
        LEASEF["lease.json<br/>who owns it"]
        MAN["manifest.json<br/>current generation"]
        SNAP["gen/N/snapshot/…<br/>state at one LSN"]
        SEG["gen/N/log/…<br/>frames after the snapshot"]
    end
    SEG -- "pull applies frames" --> DATA
    SNAP -- "cold boot loads" --> DATA
    OUT -- "push" --> SEG
```

Everything on the left except the outbox can be rebuilt from the right. The outbox is
the only local data that exists nowhere else, which is why a rebuild keeps it.

## Reads

A read never leaves the process.

1. `Database::get` asks the manager for the open local store, reopening it if it was
   closed to make room.
2. The overlay is checked first: a pending buffered write to that key wins.
3. Otherwise the replicated value is returned.
4. If the database is encrypted, the value is opened with the caller's key.

Scans (`range`, `prefix`) merge the overlay and the data in key order and skip deleted
keys. An iterator keeps its database open until it is dropped.

## Writes

```mermaid
sequenceDiagram
    participant App as Application
    participant Db as Database (valkvedge)
    participant W as Writer (valkvedge-sync)
    participant S as Object store

    Note over App,S: Sync write
    App->>Db: put, Durability::Sync
    Db->>W: commit(changes)
    W->>S: create-only PUT gen/N/log/SEQ
    S-->>W: stored
    W->>Db: apply frame to local store
    Db-->>App: lsn

    Note over App,S: Buffered write
    App->>Db: put, Durability::Buffered
    Db->>Db: one local batch: outbox entry + overlay
    Db-->>App: outbox sequence number
    Note over Db: later: every push_interval, at push_bytes, or on flush()
    Db->>W: push(device, outbox entries)
    W->>W: per key, keep the newer timestamp
    W->>S: create-only PUT (one frame per entry)
    W->>Db: apply winning operations
    Db->>Db: remove settled outbox and overlay entries
```

Two properties to notice:

- **A sync write touches local state only after the object store has it.** A failed
  sync write leaves nothing to undo.
- **A buffered write needs no network at all.** It does not even take the lease. The
  lease is taken when the outbox is pushed.

### Conflicts

Every operation carries a hybrid logical clock timestamp: wall-clock milliseconds, a
counter, and the device id. Sync writes are stamped by the writer at commit, later than
anything it has seen. Buffered writes are stamped by the device when they are made.

When the writer receives pushed operations it compares each one with what it holds for
that key and keeps the newer. The log contains only winners; replicas apply it in order
and never re-decide. The push result says, per operation, whether it was applied,
superseded, or rejected:

- stamped more than `max_clock_skew` in the future: rejected, so a device with a wrong
  clock cannot win every future conflict;
- older than `tombstone_retention`: rejected, because the writer may no longer remember
  a delete it would need to compare against.

The device's highest applied outbox entry (its *watermark*) is recorded in the log
itself. A push that is retried, even against a different writer after a failover, is
recognised and not applied twice.

## One writer at a time

Two separate mechanisms, on purpose.

**The lease decides who should write.** `lease.json` names the owning node. It is created
with a create-only write and changed only by compare-and-swap. Each node renews a small
heartbeat object every few seconds. Another node may take a lease only when the owner
released it, restarted, or its heartbeat has stood still for the lease TTL as measured
on the watcher's own clock. Clocks on different machines are never compared.

**Fencing makes a wrong belief harmless.** A node can lose its lease without knowing: it
was paused, or cut off from the store. So ownership is also enforced by the log:

- A segment's key is its sequence number, and segments are written create-only. Two
  writers cannot both occupy a position.
- A new owner immediately writes an empty *marker* segment at the next position and
  rewrites the manifest. The previous owner's next append hits an occupied position; its
  next compaction fails its compare-and-swap.
- A node whose own heartbeat is overdue refuses to write, checked before the upload and
  again before acknowledging.

```mermaid
sequenceDiagram
    participant A as Node A (owner, then paused)
    participant S as Object store
    participant B as Node B

    A->>S: PUT log/5 (create-only)  ✓
    Note over A: paused, heartbeat stops
    B->>S: watch A's heartbeat for the lease TTL: unchanged
    B->>S: compare-and-swap lease.json → B  ✓
    B->>S: compare-and-swap manifest.json  ✓
    B->>S: PUT log/6 marker (create-only)  ✓
    B->>S: PUT log/7 (create-only)  ✓
    Note over A: resumes, still believes it owns the database
    A->>S: PUT log/6 (create-only)  ✗ already exists
    Note over A: fenced: fails the request, must reopen
```

The heartbeat runs on its own thread with its own runtime, so a busy or blocked
application runtime cannot make a healthy node look dead.

## Replicas and generations

`pull` reads the manifest and then:

- **Same generation:** reads the segments after its position, checks each one chains to
  the previous by checksum and continues at the next LSN, and applies them.
- **One generation behind, old segments still present:** finishes the old generation
  from frames, then moves to the new one.
- **Empty, further behind, or the old segments are gone:** discards replicated state,
  loads the current snapshot, then applies the segments after it.

Applying a segment and recording the new position are one atomic local write, so
replaying after a crash changes nothing. A snapshot load that is interrupted leaves a
mark; the next pull sees it and starts the load again.

Compaction is the writer writing a snapshot of its own state, in bounded parts, then
swapping the manifest to a new generation. Deletes older than the retention window are
left out. The previous generation is kept for a grace period for replicas that are
slightly behind, then deleted.

## Many databases in one process

Each database is its own Fjall database in its own directory. The manager keeps at most
`max_open_databases` of them open.

- Opening one more when the limit is reached closes the least recently used database
  that nothing is using.
- A database is "in use" while an operation or an iterator holds it. If every open
  database is in use, the open waits up to `open_wait_timeout`, then fails with
  `TOO_MANY_OPEN_DATABASES`.
- A `Database` handle stays valid after its store was closed. The next call reopens it.
- A background task closes databases that have been idle for `idle_close_after`.

Closing a store to make room does not release its lease: the process is still alive and
still the owner. `ValkvEdge::close` releases leases.

## Encryption

With a key, each value is sealed with ChaCha20-Poly1305 under a random nonce before it
becomes part of an operation. From then on the outbox, the overlay, the local data, the
log segments and the snapshots all hold ciphertext. The database name and the key (name)
are bound into the seal, so a ciphertext copied to another key or database does not open.

Key names are not encrypted, because ordered scans and conflict resolution need them.

A sealed marker is stored in the manifest and locally. Opening with the wrong key, or
with no key, fails with `INVALID_ENCRYPTION_KEY` before any value is read.

## Errors

One error type, `valkvedge::ValkvEdgeError`, with a stable `code()` for programs and a
message for people. The codes are listed in
[`error.rs`](../crates/valkvedge-log/src/error.rs).

## Not here yet

- Group commit: each sync write is currently one object.
- Bulk loading of snapshots through Fjall's ingestion API.
