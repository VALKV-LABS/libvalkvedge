# Connecting to a hosted service

Instead of giving the library an object store of your own, you can point it at a
service that owns the object store for you. The device then needs no storage
credentials: only the service's URL and an API key.

```rust
let kv = ValkvEdge::builder("/var/lib/myapp/kv")
    .remote("https://api.example.com", "ekv_your_api_key")
    .build()?;
let db = kv.database("telemetry").open().await?;
```

Everything else is the same API. What changes is where things happen:

| | With your own object store | With a hosted service |
|---|---|---|
| Reads | Local | Local |
| Sync write | This process appends to the log | The service appends; the write returns when it confirms, and the local copy has caught up to it |
| Buffered write | Local outbox, pushed by this process | Local outbox, pushed to the service |
| Conflicts | Resolved by this process | Resolved by the service; losers are reported through `take_events` the same way |
| Following other devices | Reads the bucket | Asks the service for new frames |
| First open on a new device | Downloads the snapshot from the bucket | Downloads it from the service |
| Works offline | Reads and buffered writes | Reads and buffered writes |

Databases are addressed by the names they have in the service. Create them there first;
opening a name the API key cannot see fails with `NOT_FOUND`.

## What the client needs from the service

The client speaks a small HTTP API. This section is for anyone who wants to check what
the library sends, or to stand up something that speaks the same protocol. The
[reference server in the tests](../crates/valkvedge/tests/hosted.rs) implements all of
it in about 250 lines.

Every request is `Authorization: Bearer <api key>` under `/v1/db/{database}`.

| Request | Purpose | Success response |
|---|---|---|
| `POST /commit` | One atomic sync write | `{ "lsn": 42 }` |
| `POST /push` | Hand over outbox entries | `{ "watermark", "lsn", "entries": [{ "seq", "results": [...] }] }` |
| `GET /head` | Newest LSN | `{ "generation", "lsn" }` |
| `GET /log?after={lsn}` | Frames after an LSN | Binary: one [segment](format.md#segment), with the generation in `x-edgekv-generation` |
| `GET /snapshot` | Current snapshot | `{ "generation", "snapshot_lsn", "parts": [{ "url", "size", "checksum" }] }` |
| `GET {part url}` | One snapshot part | Binary: one [snapshot part](format.md#snapshot-part) |

Keys and values travel base64-encoded in JSON bodies.

**Which device is asking.** On `GET /head` and `GET /log` the library also sends
`X-Edgekv-Device: <uuid>`: the id this installation has for the database, the same one
its pushes carry as `device_id`. Together with `after`, it tells the service how far
each device has read, so it can show which devices follow a database and how far
behind each is. The header is optional to a service that speaks this protocol; a
request without it is answered the same way.

**Commit**

```json
{ "ops": [
  { "op": "put", "key": "YQ==", "value": "MQ==" },
  { "op": "delete", "key": "Yg==" }
] }
```

**Push**

```json
{ "device_id": "0f0f0f0f-0f0f-4f0f-8f0f-0f0f0f0f0f0f",
  "entries": [
    { "seq": 7, "ops": [
      { "op": "put", "key": "YQ==", "value": "MQ==", "hlc": { "ms": 1791700000000, "counter": 0 } }
    ] }
  ] }
```

Each result is one of `applied`, `superseded`, `duplicate`, `clock_skew`,
`stale_write`, in the order of the operations. A push is safe to repeat: entries at or
below the device's watermark come back as `duplicate` and are not applied again.

**Log.** A page may hold fewer frames than exist; the client asks again from the last
LSN it applied until a page comes back empty. If the frames asked for now exist only
inside the snapshot, the service answers `409` with code `SNAPSHOT_REQUIRED`, and the
client rebuilds its local copy from the snapshot. Buffered writes that have not been
pushed yet are kept through a rebuild.

**Snapshot.** If a newer snapshot replaces the one being downloaded, a part request
answers `409` with code `SNAPSHOT_CHANGED` and the client starts over from the listing.

## Errors

A failed request carries a code in the `x-edgekv-error` header and in the body:

```json
{ "error": { "code": "RATE_LIMITED", "message": "too many requests for this API key" } }
```

The library turns these into `ValkvEdgeError`, and `code()` returns the same string.

| Code | What it means for your program |
|---|---|
| `UNAUTHENTICATED` | The API key is missing, wrong or revoked |
| `FORBIDDEN` | The key may not do this, for example a read-only key writing |
| `NOT_FOUND` | No such database for this key |
| `ACCOUNT_SUSPENDED` | The account cannot be used at the moment |
| `DATABASE_NOT_READY` | The database exists but cannot take this request yet |
| `STORAGE_QUOTA_EXCEEDED` | The account is over its storage limit; reads and deletes still work |
| `RATE_LIMITED` | Slow down and retry |
| `KEY_TOO_LARGE`, `VALUE_TOO_LARGE`, `BATCH_TOO_LARGE` | The same limits the library enforces locally |
| `LEASE_HELD` | The database is changing hands; retry shortly |
| `UNAVAILABLE` | The service could not be reached, or it could not reach its storage. Sync writes fail; reads and buffered writes carry on locally |

A code the library does not recognise is reported as `SERVICE_ERROR` with the service's
message.

## What to know

- **Timeouts.** A commit or a push waits at most `Options::sync_write_timeout`
  (10 seconds by default), then fails with `UNAVAILABLE`. As with any network write,
  the service may have stored it anyway; make sync writes safe to repeat.
- **Encryption** works the same way: with `encryption_key`, values are sealed on the
  device and the service only ever stores ciphertext. Key names are not encrypted. The
  check that catches a wrong key before any read is kept on the device only in this
  mode; a device opening with the wrong key finds out on its first read.
- **One device, one outbox.** The device id that identifies a device's pushes is
  stored in its local directory. Copying that directory to another machine makes two
  devices with the same id; do not do that.
- **Not available in this mode:** `compact`, `take_over`, `owner`, `accept_push` and
  namespaces. They belong to a process that owns the object store.
