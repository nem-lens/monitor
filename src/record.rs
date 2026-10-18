//! The private event store: one full event record per event, as the event
//! schema lays it out (`nem_lens_aemo::event`):
//!
//! ```text
//! STORE/<id>/manifest.json   AEMO sources and SHA-256
//! STORE/<id>/bundle/...      AEMO tables as fetched (Parquet) and market notices
//! ```
//!
//! The store holds AEMO data only. Nothing the engine computes is written
//! here (owner, 19 Oct 2026): replays, counterfactuals and attribution are
//! computed on the user's own machine from these files.

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};

use crate::bundle::{Manifest, MANIFEST};
use crate::extract::NOTICES_FILE;

/// Copy the bundle at `bundle` into the store at `store`, replacing any
/// earlier record of the event. Only the bundle's AEMO files are copied:
/// subfolders (such as an engine's `replay/` output) are left behind.
pub fn put(bundle: &Path, store: &Path) -> Result<PathBuf> {
    let m = Manifest::load(bundle)?;
    let id = m.event_id.as_str();
    if id.is_empty()
        || !id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        bail!("unexpected event id {id:?}");
    }
    let dir = store.join(id);
    let tables = dir.join("bundle");
    if dir.exists() {
        fs::remove_dir_all(&dir)?;
    }
    fs::create_dir_all(&tables)?;
    for e in fs::read_dir(bundle).with_context(|| format!("reading {}", bundle.display()))? {
        let p = e?.path();
        let name = p
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        if !p.is_file() {
            continue;
        }
        if name.ends_with(".parquet") || name == NOTICES_FILE {
            fs::copy(&p, tables.join(&name))?;
        }
    }
    fs::copy(bundle.join(MANIFEST), dir.join(MANIFEST))?;
    Ok(dir)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_aemo_files_reach_the_store() {
        let root = std::env::temp_dir().join(format!("nem-record-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let b = root.join("bundles").join("e1");
        fs::create_dir_all(b.join("replay")).unwrap();
        fs::write(b.join("dispatch_price.parquet"), b"x").unwrap();
        fs::write(b.join(NOTICES_FILE), b"[]").unwrap();
        fs::write(b.join("replay").join("baseline.json"), b"{}").unwrap();
        fs::write(b.join("notes.txt"), b"?").unwrap();
        let m = serde_json::json!({
            "format": 1, "generator": "t", "event_id": "e1", "region": "NSW1",
            "window_start": "2026-10-12T12:20:00+10:00", "window_end": "2026-10-12T16:35:00+10:00",
            "intervals_expected": 52, "intervals_found": 52, "created_at": "x", "updated_at": "x",
            "sources": [], "outputs": [], "next_day": {"status": "pending", "trading_days": [], "note": ""}, "notes": ""
        });
        fs::write(b.join(MANIFEST), serde_json::to_vec(&m).unwrap()).unwrap();
        let dir = put(&b, &root.join("store")).unwrap();
        assert!(dir.join("bundle").join("dispatch_price.parquet").exists());
        assert!(dir.join("bundle").join(NOTICES_FILE).exists());
        assert!(dir.join(MANIFEST).exists());
        assert!(!dir.join("bundle").join("replay").exists());
        assert!(!dir.join("bundle").join("notes.txt").exists());
        fs::remove_dir_all(&root).unwrap();
    }
}
