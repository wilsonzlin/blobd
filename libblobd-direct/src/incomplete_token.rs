use crate::objects::ObjectId;
use serde::Deserialize;
use serde::Serialize;
use std::fmt;
use std::fmt::Debug;

// This is intentionally an opaque token. To allow transporting it for library implementers, Serialize and Deserialize are derived.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IncompleteToken {
  // WARNING: This will change after committing, so cannot be depended on. Currently, `IncompleteToken` is opaque so this is not an issue.
  pub(crate) object_id: ObjectId,
  pub(crate) partition_idx: usize,
}

impl Debug for IncompleteToken {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("IncompleteToken").finish()
  }
}
