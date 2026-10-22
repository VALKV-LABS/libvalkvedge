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
arrive from the service. With the background task running, an open database is
followed: the library asks the service for what comes after its copy and the service
answers as soon as there is something (see *Waiting for changes* below), so changes
arrive within moments of being written. Against a service that does not hold requests,
the library polls every `pull_interval` instead.

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

`prefixes` limits a token to keys under them, for data shared by a team where each
user may see only part of it:

```json
{ "scope": "write", "prefixes": ["teams/42/", "users/alice/"] }
```

At most 16, each 1 to 1,024 bytes of UTF-8 text. Such a token reads, writes and scans
only those keys; anything else is `403` `FORBIDDEN`. A replica using it must be a
partial copy within them (see below): following a database with it asks the service
for those prefixes, and asking for more, or for the whole database, is refused.

## Partial copies

A copy can hold only some prefixes:

```rust
let team = kv.database("workspace").prefixes(["teams/42/"]).open().await?;
team.get(b"teams/42/board")?;        // local, as usual
team.get(b"teams/7/board");          // Err: NOT_REPLICATED
team.fetch(b"teams/7/board").await?; // from the service, if the credential allows it
```

The service sends only keys under the prefixes, in the log and in the snapshot, so a
partial copy downloads, stores and is sent only what it holds. Reads, scans, watches and
writes outside them fail with `NOT_REPLICATED`; a scan must lie inside one prefix. With
a hosted service the prefixes must be UTF-8 text. Opening the copy again with other
prefixes downloads it again. A database opened for ingestion cannot also be partial.

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
new token. Local reads never need a token.

## Reading from the service, and writing without a copy

`Database::fetch(key)`, `fetch_versioned` and `fetch_page(prefix, limit, page_token)`
read from the service rather than the local copy: the newest stored value, at the cost
of a request. Keys and prefixes read this way must be UTF-8 text, since they travel in
the URL. Encrypted values are opened on the replica as usual.

A device that mostly writes, such as a sensor, needs no copy of the whole database.
Open it for ingestion:

```rust
let readings = kv.database("readings").ingest().open().await?;
readings.put(b"temp/2026-10-15T10:00", b"20.5").await?; // buffered: returns at once
let latest = readings.fetch(b"temp/2026-10-15T10:00").await?; // read from the service
```

Writes are buffered on local disk, uploaded in the background, and removed once the
service has them; nothing is downloaded, so the local directory holds only what has not
been uploaded yet. `get`, `prefix`, `range` and `watch` fail with `UNSUPPORTED`; read
with `fetch`. Sync and conditional writes, and `update`, go straight to the service. A
database is opened one way per process: for ingestion or not.

## History and restores

A service may keep each database's history for a while. `Database::history()` says how
far back it reaches, and `Database::restore(point)` asks the service to put the database
back as it was at that point, an LSN or a time:

```rust
use valkvedge::Point;
let reach = db.history().await?;                       // from_lsn, from_ms, to_lsn
db.restore(Point::Time(an_hour_ago_ms)).await?;        // every copy reloads
```

The local copy reloads before `restore` returns; other replicas reload when they next
follow the database. A restore needs a credential that may write; a service may refuse
it to access tokens. Branching a database, a copy that goes its own way, is the
service's own business and not part of this API.

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
| `GET /log?after={lsn}&wait_ms={ms}` | Frames after an LSN; `wait_ms` is optional | Binary: one [segment](format.md#segment), with the generation in `x-edgekv-generation` |
| `GET /snapshot` | Current snapshot | `{ "generation", "snapshot_lsn", "parts": [{ "url", "size", "checksum" }] }` |
| `GET {part url}` | One snapshot part | Binary: one [snapshot part](format.md#snapshot-part) |
| `GET /kv/{key}` | One value, for `fetch` | The value, with its version in `x-edgekv-version`; `404` `KEY_NOT_FOUND` if there is none |
| `GET /kv?prefix=&limit=&page_token=` | A page of keys, for `fetch_page` | `{ "items": [{ "key", "value", "version" }], "next_page_token" }`, keys and values base64 |
| `GET /history` | How far back it can be restored, for `Database::history` | `{ "from_lsn", "from", "to_lsn" }`; `from` is RFC 3339 or `null` |
| `POST /restore` | Restore to `{ "lsn" }` or `{ "at": "<RFC 3339>" }`, for `Database::restore` | `{ "from_lsn", "lsn" }`; `409` `NOT_IN_HISTORY` outside the history |
| `GET /keys` | The database's [key slots](format.md#manifestjson), for an encrypted database | `{ "slots": [{ "id", "wrapped" }], "lsn" }` |
| `PUT /keys` | Replace them: `{ "expected": [...], "slots": [...] }` | `{}`; `412` `CONDITION_FAILED` if they are no longer `expected` |

Keys and values travel base64-encoded in JSON bodies.

**Which replica is asking.** On `GET /head` and `GET /log` the library also sends
`X-Edgekv-Replica: <uuid>`: the id this installation has for the database, the same one
its pushes carry as `replica_id`. Together with `after`, it tells the service how far
each replica has read, so it can show which replicas follow a database and how far
behind each is. The header is optional to a service that speaks this protocol; a
request without it is answered the same way.

**Waiting for changes.** A replica that is up to date asks with `wait_ms`, by
default 25,000 (`Options::follow_wait`). A service that supports it holds the
request until a frame after `after` exists, then answers with it, or answers with no
frames when the wait runs out. Either way it sets `x-edgekv-waited` on the response,
and the replica asks again at once. A response without that header was not held:
the replica then waits `pull_interval` before asking again, so a service that does
not hold requests, or will not hold any more on that database, is polled as before.
A service may hold a request for less than `wait_ms`.

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

A put may carry `"expires_at": "2026-10-18T09:30:00.250Z"`: its value expires then (see
[components](components.md#values-that-expire)). The same field may appear on a pushed
put.

`conditions` is optional. Each names a key and the version its value must be at, or
`null` for "has no value". A value that has expired counts as no value. If any does not hold, nothing is written and the service
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

**Partial copies.** A partial copy adds `prefix={prefix}` to `GET /log` and
`GET /snapshot`, once per prefix. The service then keeps only operations on keys under
them: every frame is still sent, so LSNs stay continuous, but it may hold no
operations. The snapshot listing's part URLs carry the same prefixes, and each part is
filtered when it is downloaded, so the listing leaves out `size` and `checksum`; the
client checks the checksum each part ends with instead. Watermarks are kept.

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
| `FORBIDDEN` | The key may not do this, for example a read-only key or token writing, or a token used outside its prefixes |
| `CONDITION_FAILED` | A conditional write found a key at another version; nothing was written |
| `NOT_FOUND` | No such database for this key |
| `ACCOUNT_SUSPENDED` | The account cannot be used at the moment |
| `DATABASE_NOT_READY` | The database exists but cannot take this request yet |
| `STORAGE_QUOTA_EXCEEDED` | The account is over its storage limit; reads and deletes still work |
| `DATABASE_SIZE_EXCEEDED` | This database is over the size the service allows one database; reads and deletes still work |
| `TRANSFER_QUOTA_EXCEEDED` | The account has moved as much data as the service allows for now; reads or writes, as the message says, fail until it lifts |
| `WRITE_QUOTA_EXCEEDED` | The account has made as many writes as the service allows for now; reads and deletes still work |
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
  service keeps the database's key slots (`GET /keys`, `PUT /keys`), which reveal
  nothing without one of the keys, and no encryption marker: a key that opens none of
  the slots is refused when the database is opened. A database encrypted before key
  slots has none, and a wrong key shows on the first read.
- **One replica, one outbox.** The replica id that identifies a replica's pushes is
  stored in its local directory. Copying that directory to another machine makes two
  replicas with the same id; do not do that.
- **Not available in this mode:** `compact`, `take_over`, `owner`, `accept_push` and
  namespaces. They belong to a process that owns the object store.
