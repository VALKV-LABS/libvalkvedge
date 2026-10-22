//! Value encryption with keys supplied by the application.
//!
//! Values are sealed with the database's *data key*. Each key that may open
//! the database holds a copy of the data key sealed with it, a *key slot*,
//! stored with the database; any one of them opens it. A database encrypted
//! before key slots existed has none: its one key is its data key.

use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use rand::RngCore;
use valkvedge_log::{Error, KeySlot, Result};
use zeroize::Zeroizing;

const NONCE_LEN: usize = 12;
const CHECK_PLAINTEXT: &[u8] = b"valkvedge-encryption-check";
const CHECK_KEY: &[u8] = b"\0valkvedge/check";
/// Associated data of a key's id, computed with an all-zero nonce.
const KEY_ID_AAD: &[u8] = b"valkvedge/key-id";
/// Associated data of a key slot, followed by the database name.
const SLOT_AAD: &[u8] = b"valkvedge/data-key";

/// A key: 32 bytes.
pub type KeyBytes = [u8; 32];

/// Seals and opens values for one database, with its data key.
///
/// Each value is encrypted with ChaCha20-Poly1305 under a random nonce. The
/// database name and the key are bound as associated data, so a ciphertext
/// moved to another key or database fails to open.
pub(crate) struct Cipher {
    aead: ChaCha20Poly1305,
    database: Vec<u8>,
    /// The data key, kept to seal it for keys added later. Wiped when dropped.
    data_key: Zeroizing<KeyBytes>,
    id: String,
}

impl Cipher {
    pub(crate) fn new(data_key: &KeyBytes, database: &str) -> Self {
        Cipher {
            aead: ChaCha20Poly1305::new(Key::from_slice(data_key)),
            database: database.as_bytes().to_vec(),
            data_key: Zeroizing::new(*data_key),
            id: key_id(data_key),
        }
    }

    pub(crate) fn same_key(&self, other: &Cipher) -> bool {
        self.id == other.id
    }

    pub(crate) fn data_key(&self) -> &KeyBytes {
        &self.data_key
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

/// Names a key without revealing it: hex of the first 8 bytes of the tag the
/// key computes over an empty message, with an all-zero nonce.
pub fn key_id(key: &KeyBytes) -> String {
    let tag = ChaCha20Poly1305::new(Key::from_slice(key))
        .encrypt(
            Nonce::from_slice(&[0; NONCE_LEN]),
            Payload {
                msg: b"",
                aad: KEY_ID_AAD,
            },
        )
        .expect("an empty message seals");
    hex(&tag[..8])
}

/// A new random key.
pub(crate) fn random_key() -> Zeroizing<KeyBytes> {
    let mut key = Zeroizing::new([0u8; 32]);
    rand::thread_rng().fill_bytes(key.as_mut());
    key
}

/// A key as 64 hexadecimal characters: how recovery keys are given out, and
/// how keys are written in configuration.
pub fn key_to_hex(key: &KeyBytes) -> String {
    hex(key)
}

/// A key from 64 hexadecimal characters.
pub fn key_from_hex(text: &str) -> Result<KeyBytes> {
    let bytes = unhex(text.trim())
        .filter(|b| b.len() == 32)
        .ok_or_else(|| Error::InvalidEncryptionKey("a key is 64 hexadecimal characters".into()))?;
    let mut key = [0u8; 32];
    key.copy_from_slice(&bytes);
    Ok(key)
}

fn slot_aad(database: &str) -> Vec<u8> {
    let mut aad = SLOT_AAD.to_vec();
    aad.extend_from_slice(&(database.len() as u32).to_le_bytes());
    aad.extend_from_slice(database.as_bytes());
    aad
}

/// The data key sealed with `key`, for the database named `database`.
pub(crate) fn wrap(data_key: &KeyBytes, key: &KeyBytes, database: &str) -> Result<KeySlot> {
    let mut nonce = [0u8; NONCE_LEN];
    rand::thread_rng().fill_bytes(&mut nonce);
    let sealed = ChaCha20Poly1305::new(Key::from_slice(key))
        .encrypt(
            Nonce::from_slice(&nonce),
            Payload {
                msg: data_key,
                aad: &slot_aad(database),
            },
        )
        .map_err(|_| Error::local("encryption failed"))?;
    let mut wrapped = nonce.to_vec();
    wrapped.extend_from_slice(&sealed);
    Ok(KeySlot {
        id: key_id(key),
        wrapped: hex(&wrapped),
    })
}

/// The data key, if one of `slots` opens with `key`.
pub(crate) fn unwrap(
    slots: &[KeySlot],
    key: &KeyBytes,
    database: &str,
) -> Option<Zeroizing<KeyBytes>> {
    let aead = ChaCha20Poly1305::new(Key::from_slice(key));
    let id = key_id(key);
    // The slot named for this key first; any other that opens is as good.
    let mut ordered: Vec<&KeySlot> = slots.iter().filter(|s| s.id == id).collect();
    ordered.extend(slots.iter().filter(|s| s.id != id));
    ordered.into_iter().find_map(|slot| {
        let bytes = unhex(&slot.wrapped)?;
        if bytes.len() < NONCE_LEN {
            return None;
        }
        let (nonce, sealed) = bytes.split_at(NONCE_LEN);
        let plain = Zeroizing::new(
            aead.decrypt(
                Nonce::from_slice(nonce),
                Payload {
                    msg: sealed,
                    aad: &slot_aad(database),
                },
            )
            .ok()?,
        );
        let mut data_key = Zeroizing::new([0u8; 32]);
        if plain.len() != 32 {
            return None;
        }
        data_key.copy_from_slice(&plain);
        Some(data_key)
    })
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
    fn a_slot_gives_the_data_key_to_its_key_only_and_for_its_database_only() {
        let data_key = [3u8; 32];
        let (key, other) = ([1u8; 32], [2u8; 32]);
        let slots = vec![
            wrap(&data_key, &key, "db").unwrap(),
            wrap(&data_key, &other, "db").unwrap(),
        ];
        assert_eq!(slots[0].id, key_id(&key));
        assert_ne!(slots[0].id, slots[1].id);
        assert_eq!(*unwrap(&slots, &key, "db").unwrap(), data_key);
        assert_eq!(*unwrap(&slots, &other, "db").unwrap(), data_key);
        assert!(unwrap(&slots, &[9u8; 32], "db").is_none());
        assert!(unwrap(&slots, &key, "another").is_none());
        // A slot listed under the wrong id still opens with its key.
        let mut renamed = slots[0].clone();
        renamed.id = "0000000000000000".into();
        assert_eq!(*unwrap(&[renamed], &key, "db").unwrap(), data_key);
    }

    /// Key ids and slots are a contract too: these were computed by an
    /// independent implementation of the format.
    #[test]
    fn a_key_id_and_a_slot_made_elsewhere_in_this_format_match() {
        let key: KeyBytes = std::array::from_fn(|i| i as u8);
        assert_eq!(key_id(&key), "4110f1a6725b07a0");
        let slot = KeySlot {
            id: key_id(&key),
            wrapped: "a0a1a2a3a4a5a6a7a8a9aaabdc7aaa8c9933147a78d629cf202723244d8f016c97bfb6746c01016aadbf1dd32457e5dff8609a792135357656d81992".into(),
        };
        let data_key: KeyBytes = std::array::from_fn(|i| 0xd0 + (i % 16) as u8);
        assert_eq!(
            *unwrap(std::slice::from_ref(&slot), &key, "agent-memory").unwrap(),
            data_key
        );
        assert!(unwrap(&[slot], &key, "other-db").is_none());
    }

    #[test]
    fn keys_round_trip_as_hex_and_ids_do_not_reveal_them() {
        let key = random_key();
        assert_eq!(key_from_hex(&key_to_hex(&key)).unwrap(), *key);
        assert!(key_from_hex("abc").is_err());
        assert_eq!(key_id(&key).len(), 16);
        assert!(!key_to_hex(&key).contains(&key_id(&key)));
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
