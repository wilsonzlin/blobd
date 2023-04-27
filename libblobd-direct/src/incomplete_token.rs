use serde::Deserialize;
use serde::Serialize;
use std::fmt;
use std::fmt::Debug;

// This is intentionally an opaque token. To allow transporting it for library implementers, Serialize and Deserialize are derived.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IncompleteToken {
  pub(crate) object_id: u64,
  pub(crate) bucket_id: u64,
  // Required for writing; saves a device read.
  pub(crate) key_len: u16,
}

impl Debug for IncompleteToken {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("IncompleteToken").finish()
  }
}
