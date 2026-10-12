# libvalkvedge

A local-first embedded key-value store in safe Rust.

Reads come from a local LSM-tree (built on [Fjall](https://github.com/fjall-rs/fjall))
and never touch the network. The durable copy of each database is an append-only log in
an object store such as Amazon S3, with exactly one writer at a time, enforced by
conditional writes rather than a lock service.

> **Status: early.** The library works and is tested against an in-memory store and an
> S3 emulator. It has not been run against Amazon S3 itself, the on-store format may
> still change before 1.0, and the crates are not on crates.io yet. Nothing here has
> been load-tested.

## What it does

- **Local reads.** `get`, `range` and `prefix` are served from local disk.
- **Two write modes.** `Sync` returns once the write is in the object store. `Buffered`
  returns once it is on local disk, works offline, and is uploaded in batches.
- **One writer, safely.** A lease object and create-only log segments make it impossible
  for two processes to both extend a database's log, even if one of them is paused and
  resumes believing it is still the owner.
- **Replicas.** Any number of read-only followers. A new one starts from a snapshot, not
  from the beginning of history.
- **Many databases per process.** Thousands on disk, a bounded number open. Idle ones are
  closed and reopen on next use without the caller noticing.
- **Optional encryption.** Values are sealed with a key you supply before they are
  written anywhere. Keys (the key names) are not encrypted.

## Quick start

The crates are not published yet, so depend on the repository:

```toml
[dependencies]
valkvedge = { git = "https://github.com/VALKV-LABS/libvalkvedge" }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

```rust
use std::sync::Arc;
use valkvedge::object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use valkvedge::{Durability, ValkvEdge};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Credentials and region come from the usual AWS_* environment variables.
    let s3 = AmazonS3Builder::from_env()
        .with_bucket_name("my-bucket")
        .with_conditional_put(S3ConditionalPut::ETagMatch) // required
        .build()?;

    let kv = ValkvEdge::builder("/var/lib/myapp/kv")
        .object_store(Arc::new(s3), "myapp")
        .build()?;

    let db = kv.database("telemetry").open().await?;

    // Returns after the write is stored in S3.
    db.put(b"config/mode", b"eco").await?;

    // Returns after the write is on local disk; uploaded within a few seconds.
    let mut batch = db.batch();
    batch.put(b"sensor/1", b"21.5").put(b"sensor/2", b"19.0");
    batch.commit_with(Durability::Buffered).await?;

    // Local, no network.
    assert_eq!(db.get(b"sensor/1")?.as_deref(), Some(&b"21.5"[..]));
    for item in db.prefix(b"sensor/")? {
        let (key, value) = item?;
        println!("{} = {}", String::from_utf8_lossy(&key), String::from_utf8_lossy(&value));
    }

    kv.close().await?; // release leases so another process can take over at once
    Ok(())
}
```

Leave out `.object_store(...)` for a purely local database.

A second process follows the same database read-only:

```rust
let kv = ValkvEdge::builder("/var/lib/reader/kv")
    .object_store(Arc::new(s3), "myapp")
    .role(valkvedge::Role::Replica)
    .build()?;
let db = kv.database("telemetry").open().await?; // downloads the current snapshot
db.sync().await?;                                 // pull what is new, any time
```

## What you should know before relying on it

- **Buffered writes can be lost** if the device is lost before they are uploaded, and
  by default the last moments of them can be lost on power failure (set
  `Options::local_fsync` to trade speed for that).
- **Buffered writes can lose a conflict.** Conflicts are resolved per key by timestamp,
  last write wins. A buffered write that loses is reported through
  `Database::take_events`; it is not applied and not silently dropped.
- **A failed sync write may still have been stored.** If the object store accepts a
  write but the response is lost, the caller gets an error and the write is durable.
  Make sync writes safe to repeat.
- **Encryption does not hide keys.** Key names, value sizes and timestamps stay
  readable to anyone who can read the local files or the bucket. If you lose the
  encryption key, the data cannot be recovered.
- **One writer process per database.** Other processes are replicas. A second writer is
  refused while the first is alive, and takes over about 30 seconds after it dies, or
  immediately after it calls `close()`.
- **Amazon S3 needs conditional puts enabled** on the store you pass in, as in the
  example above.

## Crates

| Crate | What it is |
|---|---|
| [`valkvedge`](crates/valkvedge) | The library applications use. |
| [`valkvedge-sync`](crates/valkvedge-sync) | Replication: pulling a log, and the writer role. Storage-engine agnostic. |
| [`valkvedge-log`](crates/valkvedge-log) | The log format, conditional object-store access, leases and fencing. |
| [`valkvedge-cli`](crates/valkvedge-cli) | Command-line inspector for a local database. |

## Documentation

| | |
|---|---|
| [Library components](docs/components.md) | The crates, and how reads, writes, conflicts, ownership and replicas work |
| [Testing locally](docs/testing-locally.md) | Run the tests, try every feature by hand against an S3 emulator, look inside the bucket |
| [Object format](docs/format.md) | Exactly what is stored in the object store |

## Command line

```sh
valkvedge-cli status /var/lib/myapp/kv telemetry
valkvedge-cli status /var/lib/myapp/kv telemetry --store-url s3://my-bucket --prefix myapp
valkvedge-cli scan   /var/lib/myapp/kv telemetry --prefix sensor/ --limit 20
valkvedge-cli get    /var/lib/myapp/kv telemetry config/mode
valkvedge-cli check-key /var/lib/myapp/kv vault --key-hex <64 hex characters>
```

The database must not be open in another process while a command runs.

## Development

Only Docker is needed. `make` lists the commands.

```sh
make unit    # unit and in-process tests
make integ   # the same behaviour against an S3 emulator
make lint    # rustfmt and clippy
make shell   # a shell wired to the emulator, for trying things by hand
make bucket  # list what the emulator holds
make down    # remove the emulator
```

[docs/testing-locally.md](docs/testing-locally.md) walks through what to try.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.

This project uses Fjall as its local storage engine and is not affiliated with it.
