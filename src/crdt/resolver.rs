//! CRDT conflict resolution

use crate::crdt::types::CRDT;
use crate::error::Result;

/// Resolve conflicts between CRDT instances
pub fn resolve_conflict<T: CRDT>(local: &mut T, remote: &T) -> Result<()> {
    local.merge(remote);
    Ok(())
}
