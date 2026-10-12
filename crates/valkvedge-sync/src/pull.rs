//! Bringing a [`StateStore`] up to date from the object store.

use crate::state::StateStore;
use valkvedge_log::{
    read_manifest, read_segments, read_snapshot_part, ChainPos, Error, Layout, Lsn, Manifest,
    Result, Store,
};

/// What a pull did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PullOutcome {
    /// LSN the state is at after the pull.
    pub applied_lsn: Lsn,
    /// Number of frames applied from the log.
    pub frames: u64,
    /// True if the state was rebuilt from a snapshot.
    pub bootstrapped: bool,
}

const SEGMENTS_PER_ROUND: usize = 64;

async fn apply_generation<S: StateStore>(
    store: &Store,
    layout: &Layout,
    state: &S,
    generation: u64,
) -> Result<u64> {
    let mut frames = 0;
    loop {
        let pos = state.position()?;
        let segments = read_segments(
            store,
            layout,
            generation,
            pos.next_seq,
            pos.prev_checksum,
            SEGMENTS_PER_ROUND,
        )
        .await?;
        if segments.is_empty() {
            return Ok(frames);
        }
        for read in segments {
            let pos = state.position()?;
            if read.segment.first_lsn != pos.applied_lsn + 1 {
                return Err(Error::corrupt(format!(
                    "segment {} starts at LSN {} but the state is at {}",
                    read.seq, read.segment.first_lsn, pos.applied_lsn
                )));
            }
            let next = ChainPos {
                generation,
                next_seq: read.seq + 1,
                prev_checksum: read.checksum,
                applied_lsn: pos.applied_lsn.max(read.segment.last_lsn),
            };
            frames += read.segment.frames.len() as u64;
            state.apply(&read.segment.frames, next)?;
        }
    }
}

async fn bootstrap<S: StateStore>(store: &Store, state: &S, manifest: &Manifest) -> Result<()> {
    state.begin_reset()?;
    for part in &manifest.snapshot_parts {
        let records = read_snapshot_part(store, part).await?;
        state.load(&records)?;
    }
    state.finish_reset(ChainPos {
        generation: manifest.generation,
        next_seq: 0,
        prev_checksum: 0,
        applied_lsn: manifest.snapshot_lsn,
    })
}

/// Applies everything in the log that `state` does not have yet.
///
/// If the state is in an older generation, the pull first tries to finish
/// that generation from its remaining segments. If they are gone, or the
/// state is more than one generation behind, the state is rebuilt from the
/// current snapshot.
///
/// Returns [`Error::NotFound`] if the database does not exist.
pub async fn pull<S: StateStore>(store: &Store, layout: &Layout, state: &S) -> Result<PullOutcome> {
    let (manifest, _) = read_manifest(store, layout)
        .await?
        .ok_or_else(|| Error::NotFound(format!("database at {}", layout.prefix())))?;
    let mut frames = 0;
    let mut bootstrapped = false;

    let pos = state.position()?;
    // An empty state never replays history when a snapshot exists.
    let cold = pos == ChainPos::default() && manifest.generation > 0;
    let mut must_bootstrap =
        cold || state.needs_bootstrap()? || pos.generation > manifest.generation;

    if !must_bootstrap && pos.generation < manifest.generation {
        // Try to reach the snapshot LSN from the old generation's own frames.
        let reachable = pos.generation + 1 == manifest.generation;
        if reachable && pos.applied_lsn < manifest.snapshot_lsn {
            match apply_generation(store, layout, state, pos.generation).await {
                Ok(n) => frames += n,
                Err(Error::Corrupt(_)) | Err(Error::NotFound(_)) => {}
                Err(e) => return Err(e),
            }
        }
        let pos = state.position()?;
        if reachable && pos.applied_lsn == manifest.snapshot_lsn {
            state.apply(
                &[],
                ChainPos {
                    generation: manifest.generation,
                    next_seq: 0,
                    prev_checksum: 0,
                    applied_lsn: pos.applied_lsn,
                },
            )?;
        } else {
            must_bootstrap = true;
        }
    }

    if must_bootstrap {
        bootstrap(store, state, &manifest).await?;
        bootstrapped = true;
    }

    frames += apply_generation(store, layout, state, manifest.generation).await?;
    Ok(PullOutcome {
        applied_lsn: state.position()?.applied_lsn,
        frames,
        bootstrapped,
    })
}

/// The newest LSN and generation visible in the object store, without
/// applying anything.
pub async fn remote_head(store: &Store, layout: &Layout) -> Result<(u64, Lsn)> {
    let (manifest, _) = read_manifest(store, layout)
        .await?
        .ok_or_else(|| Error::NotFound(format!("database at {}", layout.prefix())))?;
    let segments = read_segments(store, layout, manifest.generation, 0, 0, usize::MAX).await?;
    let lsn = segments
        .iter()
        .map(|s| s.segment.last_lsn)
        .max()
        .unwrap_or(0)
        .max(manifest.snapshot_lsn);
    Ok((manifest.generation, lsn))
}
