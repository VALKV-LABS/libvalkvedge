//! Value encryption with a key supplied by the application.

use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use rand::RngCore;
use valkvedge_log::{Error, Result};

const NONCE_LEN: usize = 12;
const CHECK_PLAINTEXT: &[u8] = b"valkvedge-encryption-check";
const CHECK_KEY: &[u8] = b"\0valkvedge/check";

/// Seals and opens values for one database.
///
/// Each value is encrypted with ChaCha20-Poly1305 under a random nonce. The
/// database name and the key are bound as associated data, so a ciphertext
/// moved to another key or database fails to open.
pub(crate) struct Cipher {
    aead: ChaCha20Poly1305,
    database: Vec<u8>,
    fingerprint: [u8; 32],
}

impl Cipher {
    pub(crate) fn new(key: &[u8; 32], database: &str) -> Self {
        Cipher {
            aead: ChaCha20Poly1305::new(Key::from_slice(key)),
            database: database.as_bytes().to_vec(),
            fingerprint: *key,
        }
    }

    pub(crate) fn same_key(&self, other: &Cipher) -> bool {
        self.fingerprint == other.fingerprint
    }

    fn aad(&self, key: &[u8]) -> Vec<u8> {
        let mut aad = Vec::with_capacity(4 + self.database.len() + key.len());
        aad.extend_from_slice(&(self.database.len() as u32).to_le_bytes());
        aad.extend_from_slice(&self.database);
        aad.extend_from_slice(key);
        aad
    }

    pub(crate) fn seal(&self, key: &[u8], plaintext: &[u8]) -> Result<Vec<u8>> {
        let mut nonce = [0u8; NONCE_LEN];
        rand::thread_rng().fill_bytes(&mut nonce);
        let sealed = self
            .aead
            .encrypt(
                Nonce::from_slice(&nonce),
                Payload {
                    msg: plaintext,
                    aad: &self.aad(key),
                },
            )
            .map_err(|_| Error::local("encryption failed"))?;
        let mut out = Vec::with_capacity(NONCE_LEN + sealed.len());
        out.extend_from_slice(&nonce);
        out.extend_from_slice(&sealed);
        Ok(out)
    }

    pub(crate) fn open(&self, key: &[u8], stored: &[u8]) -> Result<Vec<u8>> {
        let wrong = || Error::InvalidEncryptionKey("value could not be decrypted".into());
        if stored.len() < NONCE_LEN {
            return Err(wrong());
        }
        let (nonce, sealed) = stored.split_at(NONCE_LEN);
        self.aead
            .decrypt(
                Nonce::from_slice(nonce),
                Payload {
                    msg: sealed,
                    aad: &self.aad(key),
                },
            )
            .map_err(|_| wrong())
    }

    /// A sealed marker stored with the database, used to detect a wrong key
    /// before any value is read.
    pub(crate) fn make_check(&self) -> Result<String> {
        Ok(hex(&self.seal(CHECK_KEY, CHECK_PLAINTEXT)?))
    }

    pub(crate) fn verify_check(&self, check: &str) -> Result<()> {
        let bytes =
            unhex(check).ok_or_else(|| Error::corrupt("encryption marker is not valid hex"))?;
        match self.open(CHECK_KEY, &bytes) {
            Ok(plain) if plain == CHECK_PLAINTEXT => Ok(()),
            _ => Err(Error::InvalidEncryptionKey(
                "the key does not match this database".into(),
            )),
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn unhex(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok())
        .collect()
}

/// Decides whether a handle's key is acceptable given the marker stored with
/// the database. Returns a marker to store when the database is empty and
/// should become encrypted.
pub(crate) fn reconcile(
    cipher: Option<&Cipher>,
    stored: Option<&str>,
    database_is_empty: bool,
) -> Result<Option<String>> {
    match (cipher, stored) {
        (None, None) => Ok(None),
        (Some(cipher), Some(check)) => cipher.verify_check(check).map(|_| None),
        (None, Some(_)) => Err(Error::InvalidEncryptionKey(
            "this database is encrypted; open it with its key".into(),
        )),
        (Some(cipher), None) if database_is_empty => cipher.make_check().map(Some),
        (Some(_), None) => Err(Error::InvalidEncryptionKey(
            "this database already holds unencrypted data".into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_and_tamper_detection() {
        let cipher = Cipher::new(&[7; 32], "db");
        let sealed = cipher.seal(b"k", b"secret").unwrap();
        assert_ne!(&sealed[NONCE_LEN..], b"secret");
        assert_eq!(cipher.open(b"k", &sealed).unwrap(), b"secret");

        // Moved to another key, another database, or opened with another key.
        assert!(cipher.open(b"other", &sealed).is_err());
        assert!(Cipher::new(&[7; 32], "db2").open(b"k", &sealed).is_err());
        let err = Cipher::new(&[8; 32], "db").open(b"k", &sealed).unwrap_err();
        assert_eq!(err.code(), "INVALID_ENCRYPTION_KEY");

        let mut flipped = sealed.clone();
        *flipped.last_mut().unwrap() ^= 1;
        assert!(cipher.open(b"k", &flipped).is_err());
    }

    /// The stored format is a contract: values sealed by other
    /// implementations of it must open here. This one was sealed with a fixed
    /// nonce by an independent implementation.
    #[test]
    fn a_value_sealed_elsewhere_in_this_format_opens() {
        let key: [u8; 32] = std::array::from_fn(|i| i as u8);
        let sealed = unhex(
            "a0a1a2a3a4a5a6a7a8a9aaab4dcf197f0189b4c8cc6e9071b3f75750df0d5894317f0dd9e2ece8f5",
        )
        .unwrap();
        let cipher = Cipher::new(&key, "agent-memory");
        assert_eq!(
            cipher.open(b"facts/name", &sealed).unwrap(),
            b"Ada Lovelace"
        );
        assert!(cipher.open(b"facts/other", &sealed).is_err());
        assert!(Cipher::new(&key, "other-db")
            .open(b"facts/name", &sealed)
            .is_err());
    }

    #[test]
    fn same_plaintext_seals_differently_each_time() {
        let cipher = Cipher::new(&[7; 32], "db");
        assert_ne!(
            cipher.seal(b"k", b"v").unwrap(),
            cipher.seal(b"k", b"v").unwrap()
        );
    }

    #[test]
    fn reconcile_covers_every_combination() {
        let right = Cipher::new(&[1; 32], "db");
        let wrong = Cipher::new(&[2; 32], "db");
        let check = right.make_check().unwrap();

        assert_eq!(reconcile(None, None, false).unwrap(), None);
        assert_eq!(reconcile(Some(&right), Some(&check), false).unwrap(), None);
        assert!(reconcile(Some(&wrong), Some(&check), false).is_err());
        assert!(reconcile(None, Some(&check), false).is_err());
        assert!(reconcile(Some(&right), None, true).unwrap().is_some());
        assert!(reconcile(Some(&right), None, false).is_err());
    }
}
