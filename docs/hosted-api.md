# Connecting to a hosted service

Instead of giving the library an object store of your own, you can point it at a
service that owns the object store for you. The replica then needs no storage
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
| Following other replicas | Reads the bucket | Asks the service for new frames |
| First open on a new replica | Downloads the snapshot from the bucket | Downloads it from the service |
| Works offline | Reads and buffered writes | Reads and buffered writes |

Databases are addressed by the names they have in the service. Create them there first;
opening a name the API key cannot see fails with `NOT_FOUND`.

Conditional writes (`Batch::if_version`, `Database::update`) are checked by the
service, against the newest data. If the local copy was behind and the condition
fails, the library brings the copy up to date before returning `CONDITION_FAILED`, so
the next read sees the value that won. `Database::watch` reports changes as they
arrive from the service, which with the background task running is about once per
`pull_interval`.

## Replicas you do not control: tokens

An API key is for your servers. An app on a user's phone or in a browser should not
carry one: anyone can take it out of the app. Give such replicas short-lived tokens
instead. Your backend keeps the API key, decides who the user is, and asks the service
for a token that works for one database, read-only or read-write, for a limited time:

```http
POST /v1/db/{database}/tokens
Authorization: Bearer ekv_your_api_key
Content-Type: application/json

{ "scope": "write", "ttl_seconds": 3600, "subject": "user-123" }
```

```json
{ "token": "eyJ…", "scope": "write", "expires_at": "2026-10-15T13:00:00Z" }
```

`scope` is `read` or `write` (default `read`); a read-only API key can only mint `read`
tokens. `ttl_seconds` is 60 to 86,400 (default 3,600). `subject` is optional and is
recorded in the token for your own reference. A token stops working when it expires
or when the API key that minted it is revoked. A token cannot mint tokens.

On the replica, give the library a way to get tokens from your backend instead of a key:

```rust
let kv = ValkvEdge::builder(dir)
    .remote_with_tokens("https://api.example.com", |database: String| async move {
        // Ask your own backend, which checks who is signed in.
        my_backend::edge_token(&database).await
    })
    .build()?;
```

The library asks once per database, and again a minute before the token expires (it
reads the `exp` claim of the token) or whenever the service refuses it with
`TOKEN_EXPIRED` or `UNAUTHENTICATED`; it then repeats the refused request once with the
new token. Reads never need a token: they are local.

## What the client needs from the service

The client speaks a small HTTP API. This section is for anyone who wants to check what
the library sends, or to stand up something that speaks the same protocol. The
[reference server in the tests](../crates/valkvedge/tests/hosted.rs) implements all of
it in about 250 lines.

Every request is `Authorization: Bearer <api key or token>` under `/v1/db/{database}`.

| Request | Purpose | Success response |
|---|---|---|
| `POST /commit` | One atomic sync write | `{ "lsn": 42, "versions": ["…"] }` |
| `POST /push` | Hand over outbox entries | `{ "watermark", "lsn", "entries": [{ "seq", "results": [...] }] }` |
| `GET /head` | Newest LSN | `{ "generation", "lsn" }` |
| `GET /log?after={lsn}` | Frames after an LSN | Binary: one [segment](format.md#segment), with the generation in `x-edgekv-generation` |
| `GET /snapshot` | Current snapshot | `{ "generation", "snapshot_lsn", "parts": [{ "url", "size", "checksum" }] }` |
| `GET {part url}` | One snapshot part | Binary: one [snapshot part](format.md#snapshot-part) |

Keys and values travel base64-encoded in JSON bodies.

**Which replica is asking.** On `GET /head` and `GET /log` the library also sends
`X-Edgekv-Replica: <uuid>`: the id this installation has for the database, the same one
its pushes carry as `replica_id`. Together with `after`, it tells the service how far
each replica has read, so it can show which replicas follow a database and how far
behind each is. The header is optional to a service that speaks this protocol; a
request without it is answered the same way.

**Commit**

```json
{ "ops": [
  { "op": "put", "key": "YQ==", "value": "MQ==" },
  { "op": "delete", "key": "Yg==" }
],
  "conditions": [
  { "key": "YQ==", "version": "8a1b…" },
  { "key": "Yg==", "version": null }
] }
```

`conditions` is optional. Each names a key and the version its value must be at, or
`null` for "has no value". If any does not hold, nothing is written and the service
answers `412` with code `CONDITION_FAILED` and the key, base64-encoded, in
`error.key`. `versions` in the response has one entry per operation: the version that
write gave its key. A version is 56 hex characters, the
[timestamp](format.md) the write was stamped with; the library's `Version` parses and
prints them.

**Push**

```json
{ "replica_id": "0f0f0f0f-0f0f-4f0f-8f0f-0f0f0f0f0f0f",
  "entries": [
    { "seq": 7, "ops": [
      { "op": "put", "key": "YQ==", "value": "MQ==", "hlc": { "ms": 1791700000000, "counter": 0 } }
    ] }
  ] }
```

Each result is one of `applied`, `superseded`, `duplicate`, `clock_skew`,
`stale_write`, in the order of the operations. A push is safe to repeat: entries at or
below the replica's watermark come back as `duplicate` and are not applied again.

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
| `UNAUTHENTICATED` | The API key or token is missing, wrong or revoked |
| `TOKEN_EXPIRED` | The token has expired; with `remote_with_tokens` the library gets a new one and retries |
| `FORBIDDEN` | The key may not do this, for example a read-only key or token writing |
| `CONDITION_FAILED` | A conditional write found a key at another version; nothing was written |
| `NOT_FOUND` | No such database for this key |
| `ACCOUNT_SUSPENDED` | The account cannot be used at the moment |
| `DATABASE_NOT_READY` | The database exists but cannot take this request yet |
| `STORAGE_QUOTA_EXCEEDED` | The account is over its storage limit; reads and deletes still work |
| `DATABASE_SIZE_EXCEEDED` | This database is over the size the service allows one database; reads and deletes still work |
| `TRANSFER_QUOTA_EXCEEDED` | The account has moved as much data as the service allows for now; reads or writes, as the message says, fail until it lifts |
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
  replica and the service only ever stores ciphertext. Key names are not encrypted. The
  check that catches a wrong key before any read is kept on the replica only in this
  mode; a replica opening with the wrong key finds out on its first read.
- **One replica, one outbox.** The replica id that identifies a replica's pushes is
  stored in its local directory. Copying that directory to another machine makes two
  replicas with the same id; do not do that.
- **Not available in this mode:** `compact`, `take_over`, `owner`, `accept_push` and
  namespaces. They belong to a process that owns the object store.
