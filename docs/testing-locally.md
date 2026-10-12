# Testing locally

Everything here runs on your machine with Docker and nothing else: no Rust toolchain,
no AWS account. An S3 emulator (LocalStack) stands in for the object store.

- [Run the automated tests](#run-the-automated-tests)
- [Try it by hand](#try-it-by-hand)
- [Look inside the bucket](#look-inside-the-bucket)
- [What you can and cannot test this way](#what-you-can-and-cannot-test-this-way)

## Run the automated tests

```sh
make unit     # 68 tests, no services needed
make integ    # 3 tests against the S3 emulator
make lint     # rustfmt and clippy
make down     # stop the emulator when you are done
```

The first run downloads the Rust image and compiles the dependencies. Expect several
minutes; later runs take seconds.

What the suites cover:

| Suite | Where | What it proves |
|---|---|---|
| Formats | `crates/valkvedge-log/src/format.rs`, `types.rs` | Every object round-trips; any flipped bit, truncation or newer version is rejected |
| Leases | `crates/valkvedge-log/src/lease.rs` | A live owner cannot be displaced; a dead or restarted one can |
| Log | `crates/valkvedge-log/src/log.rs` | A previous owner is fenced even if it never notices it lost the lease |
| Replication | `crates/valkvedge-sync/tests/replication.rs` | Replicas follow, cold boot from a snapshot, survive an interrupted load; conflicts resolve by last write wins; retried pushes are not applied twice across a failover |
| Embedded store | `crates/valkvedge/tests/local.rs` | The API, limits, isolation between databases, bounded open handles, encryption |
| With an object store | `crates/valkvedge/tests/replicated.rs` | Writer and replica through the public API, buffered writes offline, takeover from a dead process |
| S3 API | `crates/valkvedge/tests/s3.rs` | The same conditional writes, replication and fencing over real S3 requests |

Run one test by name:

```sh
make shell
cargo test -p valkvedge-sync -- pushed_writes_resolve
```

## Try it by hand

`make shell` starts the emulator and opens a shell where it is already configured. The
[`playground`](../crates/valkvedge/examples/playground.rs) example is a small driver for
the library: one command per process.

```sh
make shell

# inside the shell
cargo build --example playground -p valkvedge
alias kv=/target/debug/examples/playground
```

The form is `kv <local directory> <local|writer|replica> <database> <command>`.

### What you are simulating

Everything runs on one machine, but each **local directory stands for a separate
device**. `/tmp/a` and `/tmp/b` are two devices that have never heard of each other.

```
  "device a"                      S3 emulator                     "device b"
  /tmp/a/notes/   ── writes ──▶   bucket valkvedge-test   ── reads ──▶   /tmp/b/notes/
  role: writer                    playground/notes/…              role: replica
```

- A **local directory** holds a complete copy of the database in an embedded store.
  Every `get` and `scan` reads that directory and nothing else.
- The **writer** is the one process allowed to add to the database's log in the bucket.
- A **replica** is a second local copy, here in a second directory, that only ever
  reads the bucket and applies what it finds to its own files. It cannot write.
- The two never talk to each other. The bucket is the only thing they share. On real
  hardware the directories would be on different machines; nothing else would change.

So "the replica is behind" means: device b's files have not yet been updated with
something that is already in the bucket.

### 1. A sync write, and a replica that follows it

```sh
kv /tmp/a writer notes put greeting hello
```

`stored at lsn 1`. Device a took ownership of `notes` (a lease object in the bucket),
uploaded the write as one log object, and only then applied it to `/tmp/a/notes`. The
write is now in two places: the bucket and device a's disk.

```sh
kv /tmp/b replica notes scan
```

`greeting  hello`. `/tmp/b` was empty, so opening the database downloaded what the
bucket holds and built a local copy in `/tmp/b/notes`. The `scan` itself then read only
that local copy.

```sh
kv /tmp/a writer notes put later 1
kv /tmp/b replica notes status
```

`applied lsn 1`, `remote lsn 2`. Device b has not looked at the bucket since it opened,
so its copy is one write behind. A `get later` on device b would say `(not found)`:
reads are local and do not check the bucket.

```sh
kv /tmp/b replica notes sync
```

`local copy is at lsn 2`. The replica read the new log object and applied it. In an
application this happens in the background every second; the playground runs one
command per process, so you trigger it yourself.

```sh
kv /tmp/b replica notes put x y
```

`error: database is read-only`. Only the writer can change the database.

### 2. A buffered write

```sh
kv /tmp/a writer notes put-buffered draft pending   # buffered as outbox entry 1
kv /tmp/a writer notes get draft                    # pending   (readable at once)
kv /tmp/a writer notes status                       # outbox entries 1; remote lsn unchanged
kv /tmp/a writer notes flush                        # pushed 1 outbox entries
kv /tmp/b replica notes sync && kv /tmp/b replica notes get draft
```

### 3. Working offline

From a second terminal, freeze the emulator:

```sh
docker compose pause s3
```

Back in the shell:

```sh
kv /tmp/a writer notes put-buffered offline yes   # returns immediately
kv /tmp/a writer notes scan                       # reads still work
kv /tmp/a writer notes put needs-network 1        # hangs: a sync write needs the store. Ctrl-C.
```

Then `docker compose unpause s3` and:

```sh
kv /tmp/a writer notes flush                      # pushed 1 outbox entries
```

Use `pause`, not `stop`: the emulator keeps its data in memory and loses it when
stopped.

### 4. A conflict

Two writes to the same key: a buffered one made first, a sync one made after it but
stored before it.

```sh
kv /tmp/a writer notes put-buffered title draft
kv /tmp/a writer notes put title final
kv /tmp/a writer notes get title      # draft: this device still sees its own pending write
kv /tmp/a writer notes flush          # pushed 1 outbox entries
                                      #   title did not apply: Superseded
kv /tmp/a writer notes get title      # final
```

### 5. Compaction and a new device

```sh
kv /tmp/a writer notes compact        # now in generation 1
kv /tmp/c replica notes status        # a brand-new device: generation 1, started from the snapshot
```

### 6. A second writer

Each command releases the database when it exits, so another device can take over at
once:

```sh
kv /tmp/d writer notes put from second     # stored at the next lsn, after catching up
```

To see a takeover from a writer that died without releasing, kill one mid-command
(`kill -9`) and run another: it waits about 30 seconds, the default lease TTL, then
proceeds. The same scenario runs in under a second in the automated tests
(`a_second_process_takes_over_from_a_dead_one_and_loses_nothing`).

### 7. Encryption

```sh
export VALKVEDGE_KEY=$(printf 'ab%.0s' $(seq 32))       # 64 hex characters
kv /tmp/a writer vault put pin 1234
kv /tmp/e replica vault get pin                          # 1234
unset VALKVEDGE_KEY
kv /tmp/f replica vault get pin                          # error: invalid encryption key
```

Then check that the value is not in the bucket in clear (see the next section): the key
name `pin` is visible, `1234` is not.

### 8. No object store at all

```sh
kv /tmp/solo local scratch put k v
kv /tmp/solo local scratch scan
```

### 9. The command-line inspector

```sh
cargo run -q -p valkvedge-cli -- status /tmp/a notes
cargo run -q -p valkvedge-cli -- scan /tmp/a notes --limit 10
```

Local directories under `/tmp` disappear when you leave the shell. The bucket's
contents last until `make down`.

## Look inside the bucket

From the host, in the repository directory:

```sh
make bucket
```

```
        36 playground/notes/gen/0000000000/log/00000000000000000000    marker written when a writer took ownership
       135 playground/notes/gen/0000000000/log/00000000000000000001    one sync write
       188 playground/notes/gen/0000000001/snapshot/000000             snapshot from `compact`
       134 playground/notes/lease.json
       322 playground/notes/manifest.json
```

Read an object:

```sh
docker compose exec s3 awslocal s3 cp s3://valkvedge-test/playground/notes/manifest.json -
docker compose exec s3 awslocal s3 cp s3://valkvedge-test/playground/notes/lease.json -
```

[format.md](format.md) explains each object.

### Is there a UI?

Not in the emulator container itself. Two options:

- **Any S3 client with a custom endpoint.** The emulator listens on
  `http://localhost:4566` (change the port with `VALKVEDGE_S3_PORT`). Use access key
  `test`, secret `test`, region `us-east-1`, bucket `valkvedge-test`, and path-style
  addressing. For example, with the AWS CLI on your host:

  ```sh
  AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1 \
    aws --endpoint-url http://localhost:4566 s3 ls --recursive s3://valkvedge-test
  ```

- **LocalStack's own web application** has a resource browser that connects to an
  emulator running on your machine. It needs a LocalStack account; check their site for
  what your plan includes.

## What you can and cannot test this way

You can test, on one machine:

- every read and write path, both durabilities, batches and scans;
- replicas: cold boot, incremental sync, read-only enforcement;
- buffered writes with the object store unreachable, and the push afterwards;
- conflict resolution and how a losing write is reported;
- ownership: refusal while an owner is alive, immediate takeover after a clean close,
  takeover after a death, and fencing of the previous owner;
- compaction and generations, by looking at the bucket before and after;
- encryption, including what is and is not visible in the bucket;
- the open-database limit (in `tests/local.rs`; lower `max_open_databases` to see it).

You cannot conclude from a local run:

- **That it behaves the same on Amazon S3.** The emulator implements the conditional
  writes this library depends on, but it is not the real service. To run the integration
  tests against a real bucket, set `VALKVEDGE_TEST_S3_BUCKET` and the usual `AWS_*`
  variables (without `AWS_ENDPOINT`) and run
  `cargo test -p valkvedge --test s3`. The tests write under a unique `it-…` prefix and
  do not delete it.
- **Anything about speed.** Latency to an emulator on the same machine says nothing
  about a sync write to S3, and nothing here measures throughput or large databases.
- **Behaviour across machines with different clocks.** Timestamps come from one clock in
  a local run. Clock-skew handling is covered by tests that construct timestamps
  directly, not by real skew.
- **Crashes at arbitrary points.** The tests cover a dead owner and an interrupted
  snapshot load. Nothing kills a process at random instants.
