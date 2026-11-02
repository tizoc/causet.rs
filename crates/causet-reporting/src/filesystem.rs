//! The file dump: records as JSON lines, rotated by size.

use std::fs::{OpenOptions, create_dir_all, metadata, rename};
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::config::DumpConfig;
use crate::types::Record;

/// Clones share one lock: each record is encoded first, then rotation and
/// the record's single write happen under it, so concurrent dumps never
/// interleave within a line or rotate under one another.
#[derive(Clone, Debug)]
pub struct Filesystem {
    config: DumpConfig,
    rotation_counter: Arc<AtomicU64>,
    writing: Arc<Mutex<()>>,
}

impl Filesystem {
    /// A dump configured by `config`; the directory is created on the first
    /// write, not here.
    pub fn new(config: DumpConfig) -> Self {
        Self {
            config,
            rotation_counter: Arc::new(AtomicU64::new(0)),
            writing: Arc::new(Mutex::new(())),
        }
    }

    /// Appends `record` as one JSON line, first rotating the file if it has
    /// reached `max_file_size_bytes`. Writes nothing while the dump is disabled
    /// or `dir` is empty.
    pub fn dump(&self, record: &Record) -> io::Result<()> {
        if !self.config.enabled {
            return Ok(());
        }
        if self.config.dir.as_os_str().is_empty() {
            return Ok(());
        }

        let mut line = Vec::with_capacity(1024);
        record.write_json(&mut line)?;
        line.push(b'\n');

        // Nothing under the lock can leave it inconsistent.
        let _writing = self.writing.lock().unwrap_or_else(PoisonError::into_inner);
        create_dir_all(&self.config.dir)?;
        let path = self.config.dir.join(&self.config.filename);
        self.maybe_rotate(&path, record.timestamp_ms)?;
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?
            .write_all(&line)
    }

    fn maybe_rotate(&self, path: &PathBuf, timestamp_ms: u64) -> io::Result<()> {
        if self.config.max_file_size_bytes == 0 {
            return Ok(());
        }
        let size = metadata(path).map(|meta| meta.len()).unwrap_or(0);
        if size < self.config.max_file_size_bytes {
            return Ok(());
        }

        let rotated = self.rotated_path(timestamp_ms);
        if rotated == *path {
            return Ok(());
        }
        rename(path, rotated)
    }

    fn rotated_path(&self, timestamp_ms: u64) -> PathBuf {
        let filename = &self.config.filename;
        let (stem, ext) = match filename.rsplit_once('.') {
            Some((stem, ext)) => (stem, ext),
            None => (filename.as_str(), "jsonl"),
        };
        let mut counter = self.rotation_counter.fetch_add(1, Ordering::Relaxed);
        for _ in 0..1024 {
            let suffix = if counter == 0 {
                format!("{timestamp_ms}")
            } else {
                format!("{timestamp_ms}-{counter}")
            };
            let rotated = self.config.dir.join(format!("{stem}-{suffix}.{ext}"));
            if !rotated.exists() {
                return rotated;
            }
            counter = counter.saturating_add(1);
        }
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        self.config
            .dir
            .join(format!("{stem}-{timestamp_ms}-{nanos}.{ext}"))
    }
}
