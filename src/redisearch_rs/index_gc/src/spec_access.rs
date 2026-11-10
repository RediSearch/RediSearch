/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Lock access to a live spec, shared by every GC policy: the fork parent applies a
//! child's deltas through it, and the in-process GC takes its locks through it.

use index_spec::{IndexSpecWeakRef, IndexSpecWriteGuard};

use crate::GcError;

/// The closure prevents the [`IndexSpecWriteGuard`] from outliving the strong
/// reference that keeps the spec alive. Test implementations can provide the
/// same scope over an exclusively owned synthetic spec.
pub trait SpecWriteAccess {
    /// Promote and write-lock the spec, then run `apply` under the lock.
    fn with_write<T>(
        &mut self,
        apply: impl FnOnce(&mut IndexSpecWriteGuard<'_>) -> Result<T, GcError>,
    ) -> Result<T, GcError>;
}

impl SpecWriteAccess for IndexSpecWeakRef {
    fn with_write<T>(
        &mut self,
        apply: impl FnOnce(&mut IndexSpecWriteGuard<'_>) -> Result<T, GcError>,
    ) -> Result<T, GcError> {
        let spec_ref = self.promote().ok_or(GcError::SpecDeleted)?;
        let mut lock = spec_ref.write();
        let mut guard = lock.guard();
        apply(&mut guard)
    }
}
