# Object format, version 2

Everything a database keeps in the object store. Implemented in
[`valkvedge-log`](../crates/valkvedge-log/src/format.rs); the tests there are the
executable form of this document.

All integers are little-endian. A *byte string* is a `u32` length followed by that many
bytes. Checksums are CRC-32 (IEEE).

## Keys

A database lives under a prefix chosen by the caller.

```
{prefix}/lease.json
{prefix}/manifest.json
{prefix}/gen/{generation:010}/log/{seq:020}
{prefix}/gen/{generation:010}/snapshot/{part:06}
_nodes/{node_id}.json                              (store root, shared by all databases)
```

Zero-padded decimal numbers make keys sort in numeric order.

## Timestamp (HLC), 28 bytes

| Field | Type |
|---|---|
| `physical_ms` | `u64`, milliseconds since the Unix epoch |
| `counter` | `u32` |
| `replica` | 16 bytes |

Compared field by field in that order.

## Operation

| Field | Type |
|---|---|
| `kind` | `u8`: 0 put, 1 delete, 2 put of a value that expires (version 2) |
| `key` | byte string |
| `value` | byte string (empty for a delete; ciphertext if the database is encrypted) |
| `hlc` | timestamp |
| `expires_ms` | `u64`, milliseconds since the Unix epoch. Kind 2 only |

A value that has expired reads as absent. The writer then deletes it with an ordinary
kind 1 operation.

## Frame

One atomic batch. Applied completely or not at all.

| Field | Type |
|---|---|
| `lsn` | `u64` |
| `commit_hlc` | timestamp |
| `origin` | `u8` 0, or `u8` 1 followed by `replica` (16 bytes), `first_seq` `u64`, `last_seq` `u64` |
| `op_count` | `u32` |
| operations | `op_count` operations |

`origin` is present on frames built from a replica's outbox. A frame may have zero
operations and exist only to record `origin`.

## Segment

One object: a contiguous run of frames.

| Field | Type |
|---|---|
| magic | `VKSG` |
| `version` | `u16` |
| flags | `u16`, zero |
| `first_lsn` | `u64` |
| `last_lsn` | `u64` |
| `prev_checksum` | `u32`: checksum of the previous segment in the generation, 0 for the first |
| `frame_count` | `u32` |
| frames | per frame: `u32` length, `u32` checksum of the frame bytes, the frame |
| checksum | `u32` over everything before it |

A segment's key is its sequence number in the generation. Segments are written
create-only, so a position can be written once.

A *marker* is a segment with no frames, `first_lsn` set to the next LSN and `last_lsn`
one less. A new owner writes one to occupy the next position.

A reader must stop and report corruption if a sequence number is missing, a checksum
fails, `prev_checksum` does not match the previous segment, or `first_lsn` is not one
more than the last LSN it has applied.

## Snapshot part

The state of a database at one LSN, split into parts.

| Field | Type |
|---|---|
| magic | `VKSN` |
| `version` | `u16` |
| `part` | `u32` |
| `record_count` | `u32` |
| records | see below |
| checksum | `u32` over everything before it |

Records:

- tag `0`, an entry: `tombstone` `u8`, timestamp, `key` byte string, `value` byte string.
  Entries appear in ascending key order across the parts.
- tag `2`, an entry whose value expires (version 2): the same fields as tag `0`, then
  `expires_ms` `u64`.
- tag `1`, a replica watermark: `replica` (16 bytes), `seq` `u64`: the highest outbox
  sequence number applied for that replica.

## `manifest.json`

```json
{
  "format_version": 1,
  "generation": 3,
  "snapshot_lsn": 1042,
  "snapshot_parts": [{ "key": "…/gen/0000000003/snapshot/000000", "size": 8388123, "checksum": 2890133742 }],
  "owner_epoch": 7,
  "superseded": [{ "generation": 2, "at_ms": 1791700000000, "snapshot_lsn": 900,
                   "snapshot_ms": 1791600000000, "snapshot_parts": [] }],
  "encryption_check": "…hex…",
  "snapshot_ms": 1791700000000
}
```

Replaced only by compare-and-swap on its entity tag. `generation` is current;
`snapshot_lsn` 0 with no parts means the generation starts empty. `encryption_check` is
present when values are encrypted.

`superseded` lists older generations not yet deleted. Each records when it was
replaced (`at_ms`) and, for the history (see [components](components.md#history)), the
LSN and time of its snapshot and, while that snapshot is kept, its parts. `restored:
true`, here or at the top level, marks a snapshot installed by a restore or a branch,
one LSN past the tail of the log before it: no log leads to it. `snapshot_ms` is when
the current snapshot was installed. All of these fields are optional: manifests
without them read as before, and generations without `snapshot_lsn` are not part of
the history.

## `lease.json`

```json
{ "state": "held", "node_id": "node-a", "node_epoch": 1791700000000, "lease_epoch": 7, "node_addr": "10.0.0.5:9000" }
```

Created with a create-only write; changed only by compare-and-swap, with `lease_epoch`
increased by one. `state` is `held` or `released`.

## `_nodes/{node_id}.json`

```json
{ "node_epoch": 1791700000000, "seq": 1289, "addr": "10.0.0.5:9000" }
```

Rewritten every heartbeat interval with `seq` increased. `node_epoch` is fixed for the
life of a process and larger for every restart.

A lease is stale when its owner's heartbeat shows a larger `node_epoch` than the lease,
or when a watcher has seen the heartbeat unchanged for the lease TTL on the watcher's
own clock. Timestamps from different machines are never compared.

## Versioning

Every binary object carries a version after its magic, and the manifest carries
`format_version`. A reader that meets a version higher than it supports must refuse to
open the database.

| Version | Adds |
|---|---|
| 1 | Everything above except what version 2 adds |
| 2 | Values that expire: operation kind 2, snapshot record tag 2 |

A writer writes the lowest version an object needs: a segment or snapshot part is
version 2 only if it holds a value that expires, and the manifest stays at 1. A database
that never uses expiry stays readable by builds that know only version 1. A build that
knows only version 1 refuses a version 2 object rather than misreading it.
