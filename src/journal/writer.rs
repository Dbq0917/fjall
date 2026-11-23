// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::entry::{serialize_marker_item, Entry};
use crate::{
    batch::item::Item as BatchItem, file::fsync_directory, journal::recovery::JournalId,
    keyspace::InternalKeyspaceId,
};
use lsm_tree::{CompressionType, SeqNo, ValueType};
use std::{
    fs::{File, OpenOptions},
    hash::Hasher,
    io::{BufWriter, Seek, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

// TODO: this should be a database configuration
pub const PRE_ALLOCATED_BYTES: u64 = 64 * 1_024 * 1_024;

pub const JOURNAL_BUFFER_BYTES: usize = 8 * 1_024;

struct DeferredSyncState {
    file: File,
    folder: Option<PathBuf>,
}

struct DeferredSyncInner {
    state: Mutex<Option<DeferredSyncState>>,
    previous: Option<DeferredSync>,
    completed: AtomicBool,
}

/// A journal durability barrier that can be completed after the journal lock is released.
///
/// Barriers form a chain in the order they were captured. Completing a newer barrier
/// first also completes every older barrier, so table registration can never overtake
/// the fsync of an older journal.
#[derive(Clone)]
pub struct DeferredSync(Arc<DeferredSyncInner>);

impl DeferredSync {
    fn new(previous: Option<Self>, file: File, folder: Option<PathBuf>) -> Self {
        Self(Arc::new(DeferredSyncInner {
            state: Mutex::new(Some(DeferredSyncState { file, folder })),
            previous,
            completed: AtomicBool::new(false),
        }))
    }

    fn is_completed(&self) -> bool {
        self.0.completed.load(Ordering::Acquire)
    }

    /// Completes this barrier and all older barriers in the chain.
    pub(crate) fn persist(&self) -> std::io::Result<()> {
        if self.is_completed() {
            return Ok(());
        }

        #[expect(clippy::expect_used)]
        let mut state = self.0.state.lock().expect("lock is poisoned");

        if let Some(previous) = &self.0.previous {
            previous.persist()?;
        }

        let Some(resources) = state.as_ref() else {
            self.0.completed.store(true, Ordering::Release);
            return Ok(());
        };

        resources
            .file
            .sync_all()
            .inspect_err(|e| log::error!("Failed to fsync deferred journal data: {e:?}"))?;

        if let Some(folder) = &resources.folder {
            fsync_directory(folder)?;
        }

        state.take();
        self.0.completed.store(true, Ordering::Release);

        Ok(())
    }
}

pub struct Writer {
    pub(crate) path: PathBuf,
    file: BufWriter<File>,
    buf: Vec<u8>,
    is_buffer_dirty: bool,

    deferred_sync: Option<DeferredSync>,

    compression: CompressionType,
    compression_threshold: usize,
}

/// The persist mode allows setting the durability guarantee of previous writes
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum PersistMode {
    /// Flushes data to OS buffers. This allows the OS to write out data in case of an
    /// application crash.
    ///
    /// When this function returns, data is **not** guaranteed to be persisted in case
    /// of a power loss event or OS crash.
    Buffer,

    /// Flushes data using `fdatasync`.
    ///
    /// Use if you know that `fdatasync` is sufficient for your file system and/or operating system.
    SyncData,

    /// Flushes data + metadata using `fsync`.
    SyncAll,
}

impl Writer {
    pub fn set_compression(&mut self, comp: CompressionType, threshold: usize) {
        self.compression = comp;
        self.compression_threshold = threshold;
    }

    pub fn pos(&mut self) -> crate::Result<u64> {
        self.file.stream_position().map_err(Into::into)
    }

    pub fn len(&self) -> crate::Result<u64> {
        Ok(self.file.get_ref().metadata()?.len())
    }

    /// Captures a durability barrier for everything written so far.
    ///
    /// The caller must complete the returned barrier after releasing the journal lock.
    pub(crate) fn start_sync(&mut self) -> crate::Result<DeferredSync> {
        self.persist(PersistMode::Buffer)?;
        let file = self.file.get_ref().try_clone()?;

        let previous = self
            .deferred_sync
            .take()
            .filter(|sync| !sync.is_completed());
        let sync = DeferredSync::new(previous, file, None);
        self.deferred_sync = Some(sync.clone());

        Ok(sync)
    }

    /// Starts a journal rotation without fsyncing the sealed journal under the write lock.
    ///
    /// The returned barrier must be completed after the journal lock is released. The new
    /// active writer also retains the barrier so later syncs cannot overtake it.
    pub(crate) fn start_rotation(&mut self) -> crate::Result<(DeferredSync, PathBuf, PathBuf)> {
        self.persist(PersistMode::Buffer)?;

        log::debug!(
            "Sealing active journal at {}, len={}B",
            self.path.display(),
            self.path
                .metadata()
                .inspect_err(|e| {
                    log::error!(
                        "Failed to get file metadata of journal file at {}: {e:?}",
                        self.path.display()
                    );
                })?
                .len(),
        );

        let folder = self
            .path
            .parent()
            .expect("should have parent")
            .to_path_buf();

        let Some(basename) = self
            .path
            .file_name()
            .expect("should be valid file name")
            .to_str()
            .expect("should be valid utf-8")
            .strip_suffix(".jnl")
        else {
            log::error!("Invalid journal file name: {}", self.path.display());
            return Err(crate::Error::JournalRecovery(
                crate::JournalRecoveryError::InvalidFileName,
            ));
        };

        let journal_id = basename.parse::<JournalId>().map_err(|_| {
            log::error!("Invalid journal file name: {}", self.path.display());
            crate::Error::JournalRecovery(crate::JournalRecoveryError::InvalidFileName)
        })?;

        let new_path = folder.join(format!("{}.jnl", journal_id + 1));
        log::debug!("Rotating active journal to {}", new_path.display());

        let previous = self
            .deferred_sync
            .clone()
            .filter(|sync| !sync.is_completed());
        let sealed_file = self.file.get_ref().try_clone()?;
        let sealed_path = self.path.clone();

        let old_journal = {
            let mut new_journal = Self::create_new(new_path.clone())?;
            new_journal.set_compression(self.compression, self.compression_threshold);
            std::mem::replace(self, new_journal)
        };

        let sync = DeferredSync::new(previous, sealed_file, Some(folder));
        self.deferred_sync = Some(sync.clone());

        drop(old_journal);

        Ok((sync, sealed_path, new_path))
    }

    #[cfg(test)]
    pub(crate) fn rotate(&mut self) -> crate::Result<(PathBuf, PathBuf)> {
        let (sync, sealed_path, new_path) = self.start_rotation()?;
        sync.persist()?;
        Ok((sealed_path, new_path))
    }

    pub fn create_new<P: Into<PathBuf>>(path: P) -> crate::Result<Self> {
        let path = path.into();

        let file = File::create_new(&path).inspect_err(|e| {
            log::error!("Failed to create journal file at {}: {e:?}", path.display());
        })?;

        file.set_len(PRE_ALLOCATED_BYTES).inspect_err(|e| {
            log::error!(
                "Failed to set journal file size to {PRE_ALLOCATED_BYTES}B at {}: {e:?}",
                path.display(),
            );
        })?;

        // The empty file has no acknowledged writes yet. Rotation persists its directory
        // entry outside the journal lock, and later Sync* calls persist its contents.
        Ok(Self {
            path,
            file: BufWriter::new(file),
            buf: Vec::new(),
            is_buffer_dirty: false,
            deferred_sync: None,
            compression: CompressionType::None,
            compression_threshold: 0,
        })
    }

    pub fn from_file<P: AsRef<Path>>(path: P) -> crate::Result<Self> {
        let path = path.as_ref();

        if !path.try_exists()? {
            let file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(path)
                .inspect_err(|e| {
                    log::error!("Failed to create journal file at {}: {e:?}", path.display());
                })?;

            file.set_len(PRE_ALLOCATED_BYTES).inspect_err(|e| {
                log::error!(
                    "Failed to set journal file size to {PRE_ALLOCATED_BYTES}B at {}: {e:?}",
                    path.display(),
                );
            })?;

            // See `create_new`: there are no acknowledged writes to persist yet.
            return Ok(Self {
                path: path.into(),
                file: BufWriter::with_capacity(JOURNAL_BUFFER_BYTES, file),
                buf: Vec::new(),
                is_buffer_dirty: false,
                deferred_sync: None,
                compression: CompressionType::None,
                compression_threshold: 0,
            });
        }

        let file = OpenOptions::new()
            .append(true)
            .open(path)
            .inspect_err(|e| {
                log::error!("Failed to open journal file at {}: {e:?}", path.display());
            })?;

        Ok(Self {
            path: path.into(),
            file: BufWriter::with_capacity(JOURNAL_BUFFER_BYTES, file),
            buf: Vec::new(),
            is_buffer_dirty: false,
            deferred_sync: None,
            compression: CompressionType::None,
            compression_threshold: 0,
        })
    }

    /// Persists the journal file.
    pub(crate) fn persist(&mut self, mode: PersistMode) -> std::io::Result<()> {
        log::trace!(
            "Persisting journal at {} with mode={mode:?}",
            self.path.display(),
        );

        if self.is_buffer_dirty {
            self.file.flush().inspect_err(|e| {
                log::error!(
                    "Failed to flush journal IO buffers at {}: {e:?}",
                    self.path.display(),
                );
            })?;
            self.is_buffer_dirty = false;
        }

        if matches!(mode, PersistMode::SyncData | PersistMode::SyncAll) {
            if let Some(sync) = self.deferred_sync.clone() {
                sync.persist()?;
                self.deferred_sync = None;
            }
        }

        match mode {
            PersistMode::SyncAll => self.file.get_mut().sync_all().inspect_err(|e| {
                log::error!(
                    "Failed to fsync journal file at {}: {e:?}",
                    self.path.display(),
                );
            }),
            PersistMode::SyncData => self.file.get_mut().sync_data().inspect_err(|e| {
                log::error!(
                    "Failed to fsyncdata journal file at {}: {e:?}",
                    self.path.display(),
                );
            }),
            PersistMode::Buffer => Ok(()),
        }
    }

    /// Writes a batch start marker to the journal
    fn write_start(&mut self, item_count: u32, seqno: SeqNo) -> Result<usize, crate::Error> {
        debug_assert!(self.buf.is_empty());

        Entry::Start { item_count, seqno }.encode_into(&mut self.buf)?;

        self.file.write_all(&self.buf)?;

        Ok(self.buf.len())
    }

    /// Writes a batch end marker to the journal
    fn write_end(&mut self, checksum: u64) -> Result<usize, crate::Error> {
        debug_assert!(self.buf.is_empty());

        Entry::End(checksum).encode_into(&mut self.buf)?;

        self.file.write_all(&self.buf)?;

        Ok(self.buf.len())
    }

    pub(crate) fn write_raw(
        &mut self,
        keyspace_id: InternalKeyspaceId,
        key: &[u8],
        value: &[u8],
        value_type: ValueType,
        seqno: u64,
    ) -> crate::Result<usize> {
        self.is_buffer_dirty = true;

        let mut hasher = xxhash_rust::xxh3::Xxh3::default();
        let mut byte_count = 0;

        self.buf.clear();
        byte_count += self.write_start(1, seqno)?;
        self.buf.clear();

        serialize_marker_item(
            &mut self.buf,
            keyspace_id,
            key,
            value,
            value_type,
            if self.compression_threshold > 0 && value.len() >= self.compression_threshold {
                self.compression
            } else {
                CompressionType::None
            },
        )?;

        self.file.write_all(&self.buf)?;

        hasher.update(&self.buf);
        byte_count += self.buf.len();

        self.buf.clear();
        let checksum = hasher.finish();
        byte_count += self.write_end(checksum)?;

        Ok(byte_count)
    }

    pub(crate) fn write_clear(
        &mut self,
        keyspace_id: InternalKeyspaceId,
        seqno: SeqNo,
    ) -> crate::Result<usize> {
        self.is_buffer_dirty = true;

        let mut hasher = xxhash_rust::xxh3::Xxh3::default();
        let mut byte_count = 0;

        self.buf.clear();
        byte_count += self.write_start(1, seqno)?;
        self.buf.clear();

        Entry::Clear { keyspace_id }.encode_into(&mut self.buf)?;
        self.file.write_all(&self.buf)?;
        hasher.update(&self.buf);
        byte_count += self.buf.len();

        self.buf.clear();
        let checksum = hasher.finish();
        byte_count += self.write_end(checksum)?;

        Ok(byte_count)
    }

    pub fn write_batch<'a>(
        &mut self,
        items: impl Iterator<Item = &'a BatchItem>,
        batch_size: usize,
        seqno: SeqNo,
    ) -> crate::Result<usize> {
        if batch_size == 0 {
            return Ok(0);
        }

        self.is_buffer_dirty = true;

        self.buf.clear();

        // NOTE: entries.len() is surely never > u32::MAX
        #[expect(clippy::cast_possible_truncation)]
        let item_count = batch_size as u32;

        let mut hasher = xxhash_rust::xxh3::Xxh3::default();
        let mut byte_count = 0;

        byte_count += self.write_start(item_count, seqno)?;
        self.buf.clear();

        for item in items {
            debug_assert!(self.buf.is_empty());

            serialize_marker_item(
                &mut self.buf,
                item.keyspace.id,
                &item.key,
                &item.value,
                item.value_type,
                if self.compression_threshold > 0 && item.value.len() >= self.compression_threshold
                {
                    self.compression
                } else {
                    CompressionType::None
                },
            )?;

            self.file.write_all(&self.buf)?;

            hasher.update(&self.buf);
            byte_count += self.buf.len();

            self.buf.clear();
        }

        let checksum = hasher.finish();
        byte_count += self.write_end(checksum)?;

        Ok(byte_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_item(writer: &mut Writer, seqno: SeqNo) -> crate::Result<()> {
        writer.write_raw(0, b"key", b"value", ValueType::Value, seqno)?;
        Ok(())
    }

    #[test]
    fn newer_deferred_sync_completes_older_barriers() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        let mut writer = Writer::create_new(folder.path().join("0.jnl"))?;

        write_item(&mut writer, 0)?;
        let first = writer.start_sync()?;

        write_item(&mut writer, 1)?;
        let second = writer.start_sync()?;

        assert!(!first.is_completed());
        assert!(!second.is_completed());

        second.persist()?;

        assert!(first.is_completed());
        assert!(second.is_completed());

        Ok(())
    }

    #[test]
    fn explicit_sync_completes_deferred_rotation() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        let mut writer = Writer::create_new(folder.path().join("0.jnl"))?;

        write_item(&mut writer, 0)?;
        let (rotation_sync, _, _) = writer.start_rotation()?;

        assert!(!rotation_sync.is_completed());

        writer.persist(PersistMode::SyncAll)?;

        assert!(rotation_sync.is_completed());

        Ok(())
    }
}
