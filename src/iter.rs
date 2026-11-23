// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{snapshot_nonce::SnapshotNonce, Guard};
use lsm_tree::AbstractTree;
use std::any::Any;

type InnerIter = Box<dyn DoubleEndedIterator<Item = lsm_tree::IterGuardImpl> + Send + 'static>;

/// A wrapper around iterators that keep a snapshot alive
//
// We need to hold the snapshot nonce so the GC watermark does not
// move past this snapshot nonce, removing data that may still be read.
//
// The nonce only protects the MVCC watermark. Holding a pinned version as well
// keeps the tables (and blob files) that this iterator walks alive until the
// iterator is dropped, even if compaction installs a new version and deletes
// the inputs it merged. Pinning the version does not pause the compaction.
//
// Additionally, this struct also maps lsm-tree's Guards to "our" Guards.
pub struct Iter {
    iter: InnerIter,

    #[expect(unused)]
    nonce: SnapshotNonce,

    #[expect(unused)]
    pinned_version: Box<dyn Any + Send>,
}

impl Iter {
    pub(crate) fn new<T: AbstractTree>(tree: &T, nonce: SnapshotNonce, iter: InnerIter) -> Self {
        let pinned_version = tree
            .get_version_history_lock()
            .get_version_for_snapshot(nonce.instant);

        Self {
            iter,
            nonce,
            pinned_version: Box::new(pinned_version),
        }
    }
}

impl Iterator for Iter {
    type Item = crate::Guard;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(Guard)
    }
}

impl DoubleEndedIterator for Iter {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter.next_back().map(Guard)
    }
}
