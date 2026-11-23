// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

pub mod batch_reader;
pub mod entry;
pub mod error;
pub mod manager;
pub mod reader;
mod recovery;
pub mod writer;

#[cfg(test)]
mod test;

use self::writer::PersistMode;
use crate::file::fsync_directory;
use batch_reader::JournalBatchReader;
use lsm_tree::{CompressionType, SeqNo};
use reader::JournalReader;
use recovery::{recover_journals, RecoveryResult};
use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, MutexGuard,
    },
};
use writer::Writer;

pub struct Journal {
    writer: Mutex<Writer>,

    /// Next seqno that is not known to be durably persisted yet
    /// (= highest fsynced seqno + 1).
    ///
    /// Shared with the active writer, so journal rotations and direct
    /// `persist(SyncAll/SyncData)` calls advance the watermark as well.
    durable_seqno: Arc<AtomicU64>,

    /// Serializes group commit syncs, so concurrent committers are covered
    /// by a single fsync.
    sync_lock: Mutex<()>,
}

impl std::fmt::Debug for Journal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            self.path()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|_| String::from("<failed to read path>"))
        )
    }
}

impl Drop for Journal {
    fn drop(&mut self) {
        log::trace!("Dropping journal, trying to flush");

        match self.persist(PersistMode::SyncAll) {
            Ok(()) => {
                log::trace!("Flushed journal successfully");
            }
            Err(e) => {
                log::error!("Flush error on drop: {e:?}");
            }
        }

        #[cfg(feature = "__internal_whitebox")]
        crate::drop::decrement_drop_counter();
    }
}

impl Journal {
    pub fn with_compression(self, comp: CompressionType, threshold: usize) -> Self {
        {
            let mut writer = self.writer.lock().expect("lock is poisoned");
            writer.set_compression(comp, threshold);
        }
        self
    }

    fn from_file<P: AsRef<Path>>(path: P) -> crate::Result<Self> {
        let durable_seqno = Arc::new(AtomicU64::new(0));

        Ok(Self {
            writer: Mutex::new(Writer::from_file(path, durable_seqno.clone())?),
            durable_seqno,
            sync_lock: Mutex::new(()),
        })
    }

    pub fn create_new<P: AsRef<Path>>(path: P) -> crate::Result<Self> {
        let path = path.as_ref();
        log::trace!("Creating new journal at {}", path.display());

        let folder = path.parent().expect("parent should exist");

        std::fs::create_dir_all(folder).inspect_err(|e| {
            log::error!(
                "Failed to create journal folder at {}: {e:?}",
                path.display(),
            );
        })?;

        let durable_seqno = Arc::new(AtomicU64::new(0));

        let writer = Writer::create_new(path, durable_seqno.clone())?;

        // IMPORTANT: fsync folder on Unix
        fsync_directory(folder)?;

        #[cfg(feature = "__internal_whitebox")]
        crate::drop::increment_drop_counter();

        Ok(Self {
            writer: Mutex::new(writer),
            durable_seqno,
            sync_lock: Mutex::new(()),
        })
    }

    /// Hands out write access to the journal.
    ///
    /// # Errors
    ///
    /// Returns an error if the journal writer is poisoned.
    pub(crate) fn get_writer(&self) -> crate::Result<MutexGuard<'_, Writer>> {
        self.writer.lock().map_err(|_| crate::Error::Poisoned)
    }

    pub fn path(&self) -> crate::Result<PathBuf> {
        Ok(self.get_writer()?.path.clone())
    }

    pub fn get_reader(&self) -> crate::Result<JournalBatchReader> {
        let raw_reader = JournalReader::new(self.path()?)?;
        Ok(JournalBatchReader::new(raw_reader))
    }

    /// Persists the journal.
    pub fn persist(&self, mode: PersistMode) -> crate::Result<()> {
        let mut journal_writer = self.get_writer()?;
        journal_writer.persist(mode).map_err(Into::into)
    }

    /// Returns the next seqno that is not known to be durably persisted yet.
    #[doc(hidden)]
    pub fn durable_seqno(&self) -> SeqNo {
        self.durable_seqno.load(Ordering::Acquire)
    }

    /// Blocks until all journal writes up to (and including) `seqno` are
    /// durably persisted to disk.
    ///
    /// Concurrent callers are grouped into a single fsync (group commit):
    /// while one caller syncs, other callers' writes pile up in the journal
    /// buffer, and are then covered by the next sync, so the fsync rate is
    /// bounded by the fsync latency, not by the number of commits.
    ///
    /// IMPORTANT: Must not be called while holding the journal writer lock.
    pub(crate) fn sync_up_to(&self, seqno: SeqNo) -> crate::Result<()> {
        // Fast path: already durable
        if self.durable_seqno() > seqno {
            return Ok(());
        }

        let _sync_guard = self.sync_lock.lock().map_err(|_| crate::Error::Poisoned)?;

        // Another thread may have synced our write while we were waiting
        if self.durable_seqno() > seqno {
            return Ok(());
        }

        // Flush the journal buffer and get a dedicated sync handle, so the
        // (potentially slow) fsync below does not block other writers
        let (sync_file, covered_seqno) = {
            let mut journal_writer = self.get_writer()?;

            journal_writer.flush_buffer()?;

            (journal_writer.sync_handle()?, journal_writer.last_seqno())
        };

        // NOTE: `covered_seqno` may be `None` if the journal was rotated in
        // between - rotation fsyncs the previous journal, so our write is
        // durable either way
        debug_assert!(covered_seqno.is_none_or(|covered| covered >= seqno));

        sync_file.sync_data().inspect_err(|e| {
            log::error!("Failed to fsyncdata journal file: {e:?}");
        })?;

        if let Some(covered_seqno) = covered_seqno {
            self.durable_seqno
                .fetch_max(covered_seqno + 1, Ordering::AcqRel);
        }

        Ok(())
    }

    pub fn recover<P: AsRef<Path>>(
        path: P,
        compression: CompressionType,
        compression_threshold: usize,
    ) -> crate::Result<RecoveryResult> {
        recover_journals(path, compression, compression_threshold)
    }
}
