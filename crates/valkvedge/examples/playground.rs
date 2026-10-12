//! A small command-line driver for trying the library by hand.
//!
//! ```text
//! playground <root> <local|writer|replica> <database> <command> [args]
//!
//! put <key> <value>            sync write
//! put-buffered <key> <value>   buffered write (stays in the outbox until flushed)
//! del <key>                    sync delete
//! get <key>
//! scan [prefix]
//! flush                        push the outbox
//! sync                         replica: pull; writer: push the outbox
//! status
//! compact                      writer: snapshot and start a new generation
//! ```
//!
//! `writer` and `replica` use the S3 bucket named in `VALKVEDGE_TEST_S3_BUCKET`,
//! with the usual `AWS_*` variables. `local` uses no object store.
//! Set `VALKVEDGE_KEY` to 64 hex characters to encrypt values.
//!
//! See `docs/testing-locally.md` for a walkthrough.

use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use std::sync::Arc;
use valkvedge::{object_store, Durability, Options, Role, ValkvEdge};

type AnyError = Box<dyn std::error::Error>;

fn usage() -> AnyError {
    "usage: playground <root> <local|writer|replica> <database> <command> [args]\n\
     commands: put <k> <v> | put-buffered <k> <v> | del <k> | get <k> | scan [prefix] \
     | flush | sync | status | compact"
        .into()
}

fn key_from_env() -> Result<Option<[u8; 32]>, AnyError> {
    let Ok(hex) = std::env::var("VALKVEDGE_KEY") else {
        return Ok(None);
    };
    if hex.len() != 64 {
        return Err("VALKVEDGE_KEY must be 64 hex characters".into());
    }
    let mut key = [0u8; 32];
    for (i, byte) in key.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[2 * i..2 * i + 2], 16)?;
    }
    Ok(Some(key))
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    match run().await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            std::process::ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), AnyError> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [root, role, database, command, rest @ ..] = args.as_slice() else {
        return Err(usage());
    };

    // One short-lived process per command, so no background task is needed.
    let options = Options {
        background: false,
        ..Options::default()
    };
    let mut builder = ValkvEdge::builder(root).options(options);
    if role != "local" {
        let bucket = std::env::var("VALKVEDGE_TEST_S3_BUCKET")
            .map_err(|_| "set VALKVEDGE_TEST_S3_BUCKET to the bucket to use")?;
        let s3 = AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            .build()?;
        builder = builder
            .object_store(Arc::new(s3), "playground")
            .role(match role.as_str() {
                "writer" => Role::Writer,
                "replica" => Role::Replica,
                _ => return Err(usage()),
            });
    }
    let kv = builder.build()?;

    let mut open = kv.database(database);
    if let Some(key) = key_from_env()? {
        open = open.encryption_key(key);
    }
    let db = open.open().await?;

    let text = |bytes: &[u8]| String::from_utf8_lossy(bytes).into_owned();
    match (command.as_str(), rest) {
        ("put", [key, value]) => {
            let commit = db.put(key.as_bytes(), value.as_bytes()).await?;
            println!("stored at lsn {}", commit.lsn.unwrap_or(0));
        }
        ("put-buffered", [key, value]) => {
            let mut batch = db.batch();
            batch.put(key.as_bytes(), value.as_bytes());
            let commit = batch.commit_with(Durability::Buffered).await?;
            println!(
                "buffered as outbox entry {}",
                commit.outbox_seq.unwrap_or(0)
            );
        }
        ("del", [key]) => {
            let commit = db.delete(key.as_bytes()).await?;
            println!("deleted at lsn {}", commit.lsn.unwrap_or(0));
        }
        ("get", [key]) => match db.get(key.as_bytes())? {
            Some(value) => println!("{}", text(&value)),
            None => println!("(not found)"),
        },
        ("scan", rest) => {
            let prefix = rest.first().map(String::as_str).unwrap_or("");
            let mut count = 0;
            for item in db.prefix(prefix.as_bytes())? {
                let (key, value) = item?;
                println!("{}\t{}", text(&key), text(&value));
                count += 1;
            }
            println!("({count} entries)");
        }
        ("flush", []) => {
            println!("pushed {} outbox entries", db.flush().await?);
            for event in db.take_events() {
                println!("  {} did not apply: {:?}", text(&event.key), event.outcome);
            }
        }
        ("sync", []) => println!("local copy is at lsn {}", db.sync().await?),
        ("compact", []) => {
            db.compact().await?;
            println!("now in generation {}", db.status()?.generation);
        }
        ("status", []) => {
            let status = db.status()?;
            println!("applied lsn     {}", status.applied_lsn);
            println!("generation      {}", status.generation);
            println!("outbox entries  {}", status.outbox_entries);
            if let Ok(Some(remote)) = db.remote_lsn().await {
                println!("remote lsn      {remote}");
            }
        }
        _ => return Err(usage()),
    }

    // Release the lease so the next process can take over at once.
    kv.close().await?;
    Ok(())
}
