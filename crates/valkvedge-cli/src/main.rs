//! valkvedge-cli: inspect a local valkvedge database.
//!
//! The database must not be open in another process while a command runs.

#![forbid(unsafe_code)]

use clap::{Args, Parser, Subcommand};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use valkvedge::{ValkvEdge, ValkvEdgeError};
use valkvedge_log::{Layout, Store};

#[derive(Parser)]
#[command(
    name = "valkvedge-cli",
    version,
    about = "Inspect a local valkvedge database"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Args)]
struct Target {
    /// Directory that holds the local databases (the path given to the library).
    root: PathBuf,
    /// Database name.
    database: String,
    /// Encryption key as 64 hex characters. Can also be set in VALKVEDGE_KEY.
    #[arg(long, env = "VALKVEDGE_KEY", hide_env_values = true)]
    key_hex: Option<String>,
}

#[derive(Subcommand)]
enum Command {
    /// Show the applied LSN, generation, outbox depth and disk usage.
    Status {
        #[command(flatten)]
        target: Target,
        /// Object store URL (for example s3://bucket) to compare against.
        /// Credentials are read from the usual AWS_* environment variables.
        #[arg(long)]
        store_url: Option<String>,
        /// Prefix the databases live under in the object store.
        #[arg(long, default_value = "")]
        prefix: String,
    },
    /// Print one value.
    Get {
        #[command(flatten)]
        target: Target,
        /// The key.
        key: String,
    },
    /// List keys and values in order.
    Scan {
        #[command(flatten)]
        target: Target,
        /// Only keys starting with this prefix.
        #[arg(long, default_value = "")]
        prefix: String,
        /// Stop after this many entries.
        #[arg(long, default_value_t = 100)]
        limit: usize,
    },
    /// Check that an encryption key opens the database.
    CheckKey {
        #[command(flatten)]
        target: Target,
    },
}

fn parse_key(hex: &str) -> Result<[u8; 32], String> {
    let hex = hex.trim();
    if hex.len() != 64 {
        return Err("the key must be 64 hex characters (32 bytes)".into());
    }
    let mut key = [0u8; 32];
    for (i, byte) in key.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[2 * i..2 * i + 2], 16)
            .map_err(|_| "the key contains a character that is not hex".to_string())?;
    }
    Ok(key)
}

fn printable(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(s) if !s.chars().any(char::is_control) => s.to_string(),
        _ => format!(
            "0x{}",
            bytes.iter().map(|b| format!("{b:02x}")).collect::<String>()
        ),
    }
}

async fn open(target: &Target) -> Result<valkvedge::Database, String> {
    if !target.root.join(&target.database).is_dir() {
        return Err(format!(
            "no database named {:?} under {}",
            target.database,
            target.root.display()
        ));
    }
    let kv = ValkvEdge::builder(&target.root).build().map_err(describe)?;
    let mut builder = kv.database(&target.database);
    if let Some(hex) = &target.key_hex {
        builder = builder.encryption_key(parse_key(hex)?);
    }
    builder.open().await.map_err(describe)
}

fn describe(e: ValkvEdgeError) -> String {
    format!("{} ({})", e, e.code())
}

fn remote_store(url: &str) -> Result<Store, String> {
    let parsed = url::Url::parse(url).map_err(|e| format!("invalid store URL: {e}"))?;
    let mut options: Vec<(String, String)> = std::env::vars()
        .filter(|(k, _)| k.starts_with("AWS_"))
        .map(|(k, v)| (k.to_ascii_lowercase(), v))
        .collect();
    options.push(("aws_conditional_put".into(), "etag".into()));
    let (store, _) = object_store::parse_url_opts(&parsed, options)
        .map_err(|e| format!("cannot open the object store: {e}"))?;
    Ok(Store::new(Arc::from(store)))
}

async fn run(cli: Cli) -> Result<(), String> {
    match cli.command {
        Command::Status {
            target,
            store_url,
            prefix,
        } => {
            let db = open(&target).await?;
            let status = db.status().map_err(describe)?;
            println!("database        {}", db.name());
            println!("applied lsn     {}", status.applied_lsn);
            println!("generation      {}", status.generation);
            println!("outbox entries  {}", status.outbox_entries);
            println!("outbox bytes    {}", status.outbox_bytes);
            println!("disk bytes      {}", status.disk_bytes);
            if let Some(url) = store_url {
                let store = remote_store(&url)?;
                let layout =
                    Layout::new(format!("{}/{}", prefix.trim_matches('/'), target.database));
                let (generation, lsn) = valkvedge_sync::remote_head(&store, &layout)
                    .await
                    .map_err(describe)?;
                println!("remote lsn      {lsn}");
                println!("remote gen      {generation}");
                println!("behind by       {}", lsn.saturating_sub(status.applied_lsn));
            }
        }
        Command::Get { target, key } => {
            let db = open(&target).await?;
            match db.get(key.as_bytes()).map_err(describe)? {
                Some(value) => println!("{}", printable(&value)),
                None => return Err(format!("key {key:?} not found")),
            }
        }
        Command::Scan {
            target,
            prefix,
            limit,
        } => {
            let db = open(&target).await?;
            let mut shown = 0;
            for item in db.prefix(prefix.as_bytes()).map_err(describe)?.take(limit) {
                let (key, value) = item.map_err(describe)?;
                println!("{}\t{}", printable(&key), printable(&value));
                shown += 1;
            }
            eprintln!("{shown} entries");
        }
        Command::CheckKey { target } => {
            if target.key_hex.is_none() {
                return Err("give the key with --key-hex or VALKVEDGE_KEY".into());
            }
            let db = open(&target).await?;
            // Opening verified the stored marker; reading proves a value decrypts.
            if let Some(first) = db.prefix(b"").map_err(describe)?.next() {
                first.map_err(describe)?;
            }
            println!("the key opens database {}", db.name());
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> ExitCode {
    match run(Cli::parse()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("error: {message}");
            ExitCode::FAILURE
        }
    }
}
