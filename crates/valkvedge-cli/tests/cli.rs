//! Runs the built binary against a database created with the library.

use std::process::{Command, Output};
use valkvedge::ValkvEdge;

fn cli(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_valkvedge-cli"))
        .args(args)
        .env_remove("VALKVEDGE_KEY")
        .output()
        .unwrap()
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

#[tokio::test]
async fn status_get_and_scan_read_a_local_database() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    {
        let kv = ValkvEdge::builder(dir.path()).build().unwrap();
        let db = kv.database("app").open().await.unwrap();
        db.put(b"user:1", b"ada").await.unwrap();
        db.put(b"user:2", b"grace").await.unwrap();
        db.put(b"zone", b"eu").await.unwrap();
    }

    let status = cli(&["status", root, "app"]);
    assert!(status.status.success(), "{}", stderr(&status));
    assert!(stdout(&status).contains("applied lsn     3"));

    let get = cli(&["get", root, "app", "user:2"]);
    assert_eq!(stdout(&get).trim(), "grace");

    let scan = cli(&["scan", root, "app", "--prefix", "user:"]);
    assert_eq!(stdout(&scan), "user:1\tada\nuser:2\tgrace\n");

    let missing = cli(&["get", root, "app", "nope"]);
    assert!(!missing.status.success());
    let no_db = cli(&["status", root, "other"]);
    assert!(!no_db.status.success());
    assert!(stderr(&no_db).contains("no database named"));
    assert!(
        !dir.path().join("other").exists(),
        "inspecting must not create a database"
    );
}

#[tokio::test]
async fn check_key_accepts_the_right_key_and_rejects_others() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    {
        let kv = ValkvEdge::builder(dir.path()).build().unwrap();
        let db = kv
            .database("vault")
            .encryption_key([0xab; 32])
            .open()
            .await
            .unwrap();
        db.put(b"k", b"v").await.unwrap();
    }
    let right = "ab".repeat(32);
    let wrong = "cd".repeat(32);

    let ok = cli(&["check-key", root, "vault", "--key-hex", &right]);
    assert!(ok.status.success(), "{}", stderr(&ok));

    let bad = cli(&["check-key", root, "vault", "--key-hex", &wrong]);
    assert!(!bad.status.success());
    assert!(stderr(&bad).contains("INVALID_ENCRYPTION_KEY"));

    let none = cli(&["get", root, "vault", "k"]);
    assert!(stderr(&none).contains("INVALID_ENCRYPTION_KEY"));

    let malformed = cli(&["check-key", root, "vault", "--key-hex", "xyz"]);
    assert!(stderr(&malformed).contains("64 hex characters"));
}
