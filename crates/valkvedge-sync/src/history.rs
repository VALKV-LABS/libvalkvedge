//! A database's history: what it held at any point that is still kept.
//!
//! The history is one *base* snapshot and every log segment written since,
//! across generations. The state at a point is the base with the frames up
//! to that point applied. See `docs/components.md`, "History".

use crate::state::StateStore;
use valkvedge_log::{
    read_manifest, read_segments, read_snapshot_part, ChainPos, Error, Layout, Lsn, Manifest,
    PartRef, Result, Segment, Store,
};

/// A point in a database's history.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Point {
    /// Just after the frame with this LSN.
    Lsn(Lsn),
    /// The newest state at or before this time, in milliseconds since the
    /// Unix epoch, by the time each frame was committed.
    Time(u64),
}

/// How far a database's history reaches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct History {
    /// The oldest LSN the database can be restored to.
    pub from_lsn: Lsn,
    /// When the database was at `from_lsn`, in milliseconds since the Unix
    /// epoch. `None` when that is not known (history written by an older
    /// version); then only LSNs can be asked for before the first frame.
    pub from_ms: Option<u64>,
    /// The newest LSN.
    pub to_lsn: Lsn,
}

/// One generation as far as the history is concerned.
#[derive(Debug, Clone)]
struct Generation {
    generation: u64,
    snapshot_lsn: Lsn,
    snapshot_ms: Option<u64>,
    /// Its snapshot, when kept.
    parts: Option<Vec<PartRef>>,
}

/// The generations in the history, oldest first, starting at the base.
fn generations(manifest: &Manifest) -> Vec<Generation> {
    let mut all: Vec<Generation> = manifest
        .superseded
        .iter()
        .filter_map(|s| {
            Some(Generation {
                generation: s.generation,
                snapshot_lsn: s.snapshot_lsn?,
                snapshot_ms: s.snapshot_ms,
                parts: s.snapshot_parts.clone(),
            })
        })
        .collect();
    all.push(Generation {
        generation: manifest.generation,
        snapshot_lsn: manifest.snapshot_lsn,
        snapshot_ms: manifest.snapshot_ms,
        parts: Some(manifest.snapshot_parts.clone()),
    });
    all.sort_by_key(|g| g.generation);
    // The generations must follow one another; history from before a gap
    // (a generation written by an older version) cannot be replayed.
    let start = all
        .windows(2)
        .rposition(|pair| pair[1].generation != pair[0].generation + 1)
        .map_or(0, |gap| gap + 1);
    let mut all = all.split_off(start);
    let base = all.iter().position(|g| g.parts.is_some()).unwrap_or(0);
    all.split_off(base)
}

/// When a generation's snapshot was taken: known for generation 0, which
/// starts empty.
fn started(g: &Generation) -> Option<u64> {
    match g.snapshot_ms {
        Some(ms) => Some(ms),
        None if g.generation == 0 && g.snapshot_lsn == 0 => Some(0),
        None => None,
    }
}

/// The newest LSN: the end of the current generation's last segment.
async fn head(store: &Store, layout: &Layout, manifest: &Manifest) -> Result<Lsn> {
    let keys = store.list(&layout.log_prefix(manifest.generation)).await?;
    let Some(last) = keys.last() else {
        return Ok(manifest.snapshot_lsn);
    };
    let object = store
        .get(last)
        .await?
        .ok_or_else(|| Error::NotFound(last.clone()))?;
    let (segment, _) = Segment::decode(&object.bytes)?;
    Ok(segment.last_lsn.max(manifest.snapshot_lsn))
}

/// Segments read at a time while replaying, so a long history is never
/// all in memory.
const SEGMENTS_PER_READ: usize = 64;

/// How far back the database at `layout` can be restored.
pub async fn history(store: &Store, layout: &Layout) -> Result<History> {
    let (manifest, _) = read_manifest(store, layout)
        .await?
        .ok_or_else(|| Error::NotFound(format!("database at {}", layout.prefix())))?;
    let generations = generations(&manifest);
    let base = &generations[0];
    Ok(History {
        from_lsn: base.snapshot_lsn,
        from_ms: started(base),
        to_lsn: head(store, layout, &manifest).await?,
    })
}

/// Rebuilds in `scratch` the state of the database at `layout` as it was at
/// `point`, and returns the LSN that state is at. `scratch` is replaced.
///
/// Fails with `NOT_IN_HISTORY` for a point before the kept history or after
/// the newest frame.
pub async fn state_at<S: StateStore>(
    store: &Store,
    layout: &Layout,
    point: Point,
    scratch: &S,
) -> Result<Lsn> {
    let (manifest, _) = read_manifest(store, layout)
        .await?
        .ok_or_else(|| Error::NotFound(format!("database at {}", layout.prefix())))?;
    let generations = generations(&manifest);
    let head = head(store, layout, &manifest).await?;
    let oldest = &generations[0];
    // The generation the point falls in: the newest one starting at or
    // before it.
    let within = match point {
        Point::Lsn(lsn) => {
            if lsn < oldest.snapshot_lsn || lsn > head {
                return Err(Error::NotInHistory(format!(
                    "LSN {lsn} is outside {} to {head}",
                    oldest.snapshot_lsn
                )));
            }
            generations.iter().rposition(|g| g.snapshot_lsn <= lsn)
        }
        Point::Time(ms) => generations
            .iter()
            .rposition(|g| started(g).is_some_and(|start| start <= ms)),
    };
    let Some(within) = within else {
        return Err(Error::NotInHistory(
            "the time is before the oldest kept state".into(),
        ));
    };
    // Start from the newest kept snapshot at or before it: a restore's
    // snapshot is always kept, so no restore lies between it and the point.
    let base = generations[..=within]
        .iter()
        .rposition(|g| g.parts.is_some())
        .expect("the history starts with a kept snapshot");
    let first = &generations[base];
    scratch.begin_reset()?;
    for part in first.parts.iter().flatten() {
        scratch.load(&read_snapshot_part(store, part).await?)?;
    }
    let mut applied = first.snapshot_lsn;
    scratch.finish_reset(ChainPos {
        generation: first.generation,
        next_seq: 0,
        prev_checksum: 0,
        applied_lsn: applied,
    })?;
    let wanted = |frame: &valkvedge_log::Frame| match point {
        Point::Lsn(lsn) => frame.lsn <= lsn,
        Point::Time(ms) => frame.commit_hlc.physical_ms <= ms,
    };
    'generations: for g in &generations[base..=within] {
        let (mut next_seq, mut prev_checksum) = (0, 0);
        loop {
            let page = read_segments(
                store,
                layout,
                g.generation,
                next_seq,
                prev_checksum,
                SEGMENTS_PER_READ,
            )
            .await?;
            let Some(last) = page.last() else {
                break;
            };
            (next_seq, prev_checksum) = (last.seq + 1, last.checksum);
            for read in page {
                let frames: Vec<_> = read
                    .segment
                    .frames
                    .into_iter()
                    .filter(|f| f.lsn > applied)
                    .collect();
                let take = frames.iter().take_while(|f| wanted(f)).count();
                let done = take < frames.len();
                let frames = &frames[..take];
                if let Some(first) = frames.first() {
                    if first.lsn != applied + 1 {
                        return Err(Error::corrupt(format!(
                            "history jumps from LSN {applied} to {} in generation {}",
                            first.lsn, g.generation
                        )));
                    }
                    applied = frames.last().expect("not empty").lsn;
                    scratch.apply(
                        frames,
                        ChainPos {
                            generation: g.generation,
                            next_seq: 0,
                            prev_checksum: 0,
                            applied_lsn: applied,
                        },
                    )?;
                }
                if done {
                    break 'generations;
                }
            }
        }
    }
    Ok(applied)
}
