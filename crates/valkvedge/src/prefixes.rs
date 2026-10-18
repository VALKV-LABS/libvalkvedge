//! The key prefixes a partial copy holds.

use std::ops::Bound;
use valkvedge_log::types::Dec;
use valkvedge_log::{Error, Result};

/// Most prefixes one copy can hold.
pub const MAX_PREFIXES: usize = 16;

/// Key prefixes, in order, none of them starting with another.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Prefixes(Vec<Vec<u8>>);

impl Prefixes {
    /// The prefixes in `list`. `None` when one of them is empty, so the copy
    /// holds every key. A prefix inside another is dropped.
    pub(crate) fn new<P: AsRef<[u8]>>(list: impl IntoIterator<Item = P>) -> Result<Option<Self>> {
        let mut list: Vec<Vec<u8>> = list.into_iter().map(|p| p.as_ref().to_vec()).collect();
        if list.is_empty() {
            return Err(Error::Unsupported(
                "a partial copy needs at least one prefix".into(),
            ));
        }
        if list.len() > MAX_PREFIXES {
            return Err(Error::Unsupported(format!(
                "a partial copy holds at most {MAX_PREFIXES} prefixes"
            )));
        }
        if list.iter().any(|p| p.len() > crate::MAX_KEY_BYTES) {
            return Err(Error::KeyTooLarge {
                limit: crate::MAX_KEY_BYTES,
            });
        }
        if list.iter().any(Vec::is_empty) {
            return Ok(None);
        }
        list.sort();
        list.dedup();
        // Sorted, a prefix that starts with another comes right after it,
        // or after something else that starts with it.
        let mut kept: Vec<Vec<u8>> = Vec::with_capacity(list.len());
        for prefix in list {
            if !kept.last().is_some_and(|last| prefix.starts_with(last)) {
                kept.push(prefix);
            }
        }
        Ok(Some(Prefixes(kept)))
    }

    pub(crate) fn as_slice(&self) -> &[Vec<u8>] {
        &self.0
    }

    /// True when `key` is under one of the prefixes.
    pub(crate) fn holds_key(&self, key: &[u8]) -> bool {
        self.0.iter().any(|p| key.starts_with(p))
    }

    /// True when every key starting with `prefix` is held.
    pub(crate) fn holds_prefix(&self, prefix: &[u8]) -> bool {
        self.holds_key(prefix)
    }

    /// True when every key in the range is held: the range lies inside one
    /// prefix.
    pub(crate) fn holds_range(&self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> bool {
        self.0.iter().any(|p| {
            let above = match start {
                Bound::Included(s) | Bound::Excluded(s) => s >= p.as_slice(),
                Bound::Unbounded => false,
            };
            let below = match (successor(p), end) {
                (None, _) => true,
                (Some(next), Bound::Excluded(e)) => e <= next.as_slice(),
                (Some(next), Bound::Included(e)) => e < next.as_slice(),
                (Some(_), Bound::Unbounded) => false,
            };
            above && below
        })
    }

    /// The prefixes as stored in the local copy.
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut out = (self.0.len() as u32).to_le_bytes().to_vec();
        for prefix in &self.0 {
            out.extend_from_slice(&(prefix.len() as u32).to_le_bytes());
            out.extend_from_slice(prefix);
        }
        out
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        let mut dec = Dec::new(bytes);
        let count = dec.u32()? as usize;
        let mut list = Vec::with_capacity(count.min(MAX_PREFIXES));
        for _ in 0..count {
            let len = dec.u32()? as usize;
            list.push(dec.take(len)?.to_vec());
        }
        Ok(Prefixes(list))
    }
}

/// The smallest key greater than every key starting with `prefix`, or
/// `None` when there is none (the prefix is all `0xff`).
fn successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut next = prefix.to_vec();
    while let Some(last) = next.pop() {
        if last < u8::MAX {
            next.push(last + 1);
            return Some(next);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn of(list: &[&str]) -> Prefixes {
        Prefixes::new(list.iter().map(|p| p.as_bytes()))
            .unwrap()
            .unwrap()
    }

    #[test]
    fn prefixes_are_sorted_and_those_inside_another_are_dropped() {
        let p = of(&[
            "users/bob/",
            "teams/",
            "teams/42/",
            "users/alice/",
            "teams/",
        ]);
        assert_eq!(
            p.as_slice(),
            &[
                b"teams/".to_vec(),
                b"users/alice/".to_vec(),
                b"users/bob/".to_vec()
            ]
        );
        assert_eq!(Prefixes::decode(&p.encode()).unwrap(), p);
        assert_eq!(Prefixes::new(["a", ""]).unwrap(), None, "empty holds all");
        assert_eq!(
            Prefixes::new(Vec::<&str>::new()).unwrap_err().code(),
            "UNSUPPORTED"
        );
        let many: Vec<String> = (0..=MAX_PREFIXES).map(|i| format!("p{i}/")).collect();
        assert_eq!(Prefixes::new(&many).unwrap_err().code(), "UNSUPPORTED");
    }

    #[test]
    fn keys_prefixes_and_ranges_are_held_only_inside_one_prefix() {
        let p = of(&["a/", "c/"]);
        assert!(p.holds_key(b"a/1") && p.holds_key(b"a/") && p.holds_key(b"c/x/y"));
        assert!(!p.holds_key(b"a") && !p.holds_key(b"b/1"));
        assert!(p.holds_prefix(b"a/x") && !p.holds_prefix(b"") && !p.holds_prefix(b"a"));

        use Bound::*;
        let r = |s: Bound<&'static str>, e: Bound<&'static str>| {
            let b = |x: Bound<&'static str>| x.map(str::as_bytes);
            p.holds_range(b(s), b(e))
        };
        assert!(r(Included("a/"), Excluded("a0")), "exactly the prefix");
        assert!(r(Included("a/m"), Included("a/z")));
        assert!(r(Excluded("a/"), Excluded("a/q")));
        assert!(!r(Included("a/"), Included("a0")), "a0 itself is not held");
        assert!(!r(Included("a/"), Excluded("c/z")), "spans the gap");
        assert!(!r(Unbounded, Excluded("a/z")));
        assert!(!r(Included("a/"), Unbounded));
        assert!(!r(Included("0"), Excluded("a/z")), "starts before");

        let top = Prefixes::new([[0xffu8, 0xff]]).unwrap().unwrap();
        assert!(top.holds_range(Included(&[0xff, 0xff][..]), Unbounded));
        assert_eq!(successor(b"a\xff"), Some(b"b".to_vec()));
    }
}
