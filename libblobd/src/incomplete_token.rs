use crate::util::get_now_sec;
use serde::Deserialize;
use serde::Serialize;

// This is intentionally an opaque token. To allow transporting it for implementers of libblobd, Serialize and Deserialize are derived.
// SAFETY: Only DeletedList::maybe_reap_next reaps, and it reaps well after the token for an object expires (including if the object has been marked as deleted a lot earlier), so if this token hasn't expired (and our clock isn't broken), the object at `object_dev_offset` definitely still exists and its state can be safely read (e.g. to determine that it's deleted and return an error).
#[derive(Clone, Copy, Serialize, Deserialize)]
pub struct IncompleteToken {
  pub(crate) created_sec: u64,
  pub(crate) object_dev_offset: u64,
}

impl IncompleteToken {
  pub(crate) fn has_expired(&self, reap_objects_after_secs: u64) -> bool {
    get_now_sec() - self.created_sec >= reap_objects_after_secs
  }
}
