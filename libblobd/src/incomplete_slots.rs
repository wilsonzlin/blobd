use chrono::DateTime;
use chrono::TimeZone;
use chrono::Utc;
/*

INCOMPLETE SLOTS
================

Two main purposes:
- Track incomplete objects throughout device for regular GC. If we did not, expired incomplete objects would have to be released by chance, even if we inserted them into bucket linked lists and always look for expired objects when iterating buckets for any op, making some of them become "black holed" with their used space lost forever, especially as the amount of objects scale (which is a design goal).
- Provide safety for writes. It's possible for a user bug to make a write op on a committed (or worse, deleted) object. It's also possible for an object to be committed (and then even deleted) while a write op is still pending or executing. These are unlikely, but cannot be ignored if we want to be correct.
  - It's safe for multiple write ops to overlap. The user may not get what they expect when they read, but it doesn't corrupt internal state.
  - We want to allow parallel writes to different parts.
  - We don't want to make commits a bottleneck e.g. only allowing one at a time across the entire server.
  - There is no safe atomic way to read mmap data across threads, even when just reading one byte. Therefore, checking and asserting state based on inode data is perilous. It also doesn't lock/ensure that state for the duration of an op.
  - Given these rules:
    - We use an array-based map from "slot" => RwLock<Option<{ inode_dev_offset, created_time }>>.
    - Write and commit ops take a slot as part of their input. This slot is reserved by the create op and returned to the user.
    - The write op must hold a read lock. It can hold it for the entire op, or for a very tight bound (e.g. strictly while physically writing, buffering input stream and not waiting for sync, acquiring and releasing on each loop iteration). It's a RwLock to allow for parallel writes (the commit will take a write lock), so the lock hold doesn't need to be too granular as it doesn't block other writes and it's unlikely the user will commit while writing.
    - The commit op acquires a write lock, and takes the slot value, leaving None.
    - This array and the slot values are persisted to disk (it's simply the tracked list of incomplete objects), and loaded on startup, so this works even between restarts.
    - If we use a BTreeMap or HashMap, we'd have to use a RwLock over the entire data structure, which causes serialisation of all commits and blocks all writes both across the entire server. A DashMap could deadlock as we'd hold locks across awaits as part of the write op.
      - This does mean the data structure is less optimised for GC. This is an acceptable trade off, as we'd only perform GC around once a day, which is a far less frequent operation. The GC would essentially have to sequentially iterate and try lock every slot.
    - We'd have to track free slots, which may appear to sequentialise commits anyway; however, there are differences:
      - It doesn't block writes.
      - It's not required to be done before the commit is successful; we can add in the background, and it's unlikely this would lead to exhaustion of slots given the amount available.
      - We can use more optimised data structures, such as an MPMC channel.

We must also store the object ID, even though we record the inode dev offset, as an object could get deleted and its inode dev offset reused (after which some unaware client who is super late tries a write and finds it succeeds, overwriting some other object). Perhaps it's not necessary to store it on disk (storing it in memory only), and perhaps we can just store the bucket ID, but these are small byte savings not worth the hassle.

Structure
---------

{
  u64 object_id
  u64 inode_dev_offset
  u32 creation_timestamp_hours_since_epoch
}[INCOMPLETE_SLOTS_CAP] slots

*/
use off64::usz;
use off64::Off64Int;
use seekable_async_file::SeekableAsyncFile;
use std::cmp::min;
use tokio::sync::RwLock;
use tokio::sync::RwLockReadGuard;
use tokio::sync::RwLockWriteGuard;
use tracing::debug;

pub(crate) const INCOMPLETE_SLOT_OFFSETOF_OBJECT_ID: u64 = 0;
pub(crate) const INCOMPLETE_SLOT_OFFSETOF_INODE_DEV_OFFSET: u64 =
  INCOMPLETE_SLOT_OFFSETOF_OBJECT_ID + 8;
pub(crate) const INCOMPLETE_SLOT_OFFSETOF_CREATED_HOUR_TS: u64 =
  INCOMPLETE_SLOT_OFFSETOF_INODE_DEV_OFFSET + 8;
pub(crate) const INCOMPLETE_SLOT_SIZE: u64 = INCOMPLETE_SLOT_OFFSETOF_CREATED_HOUR_TS + 4;

pub(crate) const INCOMPLETE_SLOTS_CAP: u64 = 8_000_000;
pub(crate) const fn INCOMPLETE_SLOTS_OFFSETOF_SLOT(slot_id: u64) -> u64 {
  INCOMPLETE_SLOT_SIZE * slot_id
}
pub(crate) const INCOMPLETE_SLOTS_SIZE: u64 = INCOMPLETE_SLOTS_OFFSETOF_SLOT(INCOMPLETE_SLOTS_CAP);

fn from_hours_since_epoch(h: u32) -> DateTime<Utc> {
  Utc.timestamp_opt(i64::from(h) * 60 * 60, 0).unwrap()
}

// For simplicity, a vacant slot is also represented using this struct, with a `created_hour_ts` of zero. Otherwise, our IncompleteSlots methods have to return a RwLockHandle<Option<IncompleteSlot>> even though the value is always present (we checked).
pub(crate) struct IncompleteSlot {
  object_id: u64,
  inode_dev_offset: u64,
  created_hour_ts: u32,
}

impl IncompleteSlot {
  fn vacant() -> IncompleteSlot {
    IncompleteSlot {
      object_id: 0,
      inode_dev_offset: 0,
      created_hour_ts: 0,
    }
  }

  fn vacate(&mut self) {
    self.created_hour_ts = 0;
  }

  fn is_vacant(&self) -> bool {
    self.created_hour_ts == 0
  }

  #[allow(dead_code)]
  pub fn created_at(&self) -> DateTime<Utc> {
    from_hours_since_epoch(self.created_hour_ts)
  }

  pub fn inode_dev_offset(&self) -> u64 {
    // NOTE: Do not assert self is not vacant, as a slot is vacated when committing and then returned to the caller.
    self.inode_dev_offset
  }
}

pub type IncompleteSlotId = u64;

pub(crate) struct IncompleteSlots {
  slots: Vec<RwLock<IncompleteSlot>>,
  vacant_slot_ids: (flume::Sender<u64>, flume::Receiver<u64>),
}

impl IncompleteSlots {
  pub fn load_from_device(dev: &SeekableAsyncFile, dev_offset: u64) -> IncompleteSlots {
    let raw_all = dev.read_at_sync(dev_offset, INCOMPLETE_SLOTS_SIZE);
    let mut slots = IncompleteSlots {
      slots: Vec::new(),
      vacant_slot_ids: flume::unbounded(),
    };
    let mut incomplete_count = 0;
    let mut oldest = u32::MAX;
    for slot_id in 0..INCOMPLETE_SLOTS_CAP {
      let offset = slot_id * INCOMPLETE_SLOT_SIZE;
      let object_id = raw_all.read_u64_be_at(offset + INCOMPLETE_SLOT_OFFSETOF_OBJECT_ID);
      let inode_dev_offset =
        raw_all.read_u64_be_at(offset + INCOMPLETE_SLOT_OFFSETOF_INODE_DEV_OFFSET);
      let created_hour_ts =
        raw_all.read_u32_be_at(offset + INCOMPLETE_SLOT_OFFSETOF_CREATED_HOUR_TS);
      let slot = if created_hour_ts == 0 {
        slots.vacant_slot_ids.0.send(slot_id).unwrap();
        IncompleteSlot::vacant()
      } else {
        incomplete_count += 1;
        oldest = min(oldest, created_hour_ts);
        IncompleteSlot {
          object_id,
          inode_dev_offset,
          created_hour_ts,
        }
      };
      slots.slots.push(RwLock::new(slot));
    }
    debug!(
      incomplete_count,
      oldest = Some(oldest)
        .filter(|v| *v != u32::MAX)
        .map(|h| from_hours_since_epoch(h).to_string()),
      "incomplete slots loaded"
    );
    slots
  }

  pub async fn format_device(dev: &SeekableAsyncFile, dev_offset: u64) {
    dev
      .write_at(dev_offset, vec![0u8; usz!(INCOMPLETE_SLOTS_SIZE)])
      .await;
  }

  pub async fn allocate_slot(
    &self,
    mutation_writes: &mut Vec<(u64, Vec<u8>)>,
    object_id: u64,
    inode_dev_offset: u64,
  ) -> IncompleteSlotId {
    let id = self.vacant_slot_ids.1.try_recv().unwrap();
    // This may be blocked if we're acquiring a slot that has just been vacated but is still being committed (and the lock is still being held).
    let mut slot = self.slots[usz!(id)].write().await;
    assert!(slot.is_vacant());
    let now = Utc::now().timestamp();
    let created_hour_ts: u32 = (now / 60 / 60).try_into().unwrap();
    *slot = IncompleteSlot {
      object_id,
      inode_dev_offset,
      created_hour_ts,
    };
    let mut raw = vec![0u8; usz!(INCOMPLETE_SLOT_SIZE)];
    raw.write_u64_be_at(INCOMPLETE_SLOT_OFFSETOF_OBJECT_ID, object_id);
    raw.write_u64_be_at(INCOMPLETE_SLOT_OFFSETOF_INODE_DEV_OFFSET, inode_dev_offset);
    raw.write_u32_be_at(INCOMPLETE_SLOT_OFFSETOF_CREATED_HOUR_TS, created_hour_ts);
    mutation_writes.push((INCOMPLETE_SLOTS_OFFSETOF_SLOT(id), raw));
    id
  }

  pub async fn lock_slot_for_writing(
    &self,
    slot_id: IncompleteSlotId,
    expected_object_id: u64,
  ) -> Option<RwLockReadGuard<IncompleteSlot>> {
    let slot = self.slots.get(usz!(slot_id))?.read().await;
    if slot.is_vacant() || slot.object_id != expected_object_id {
      return None;
    };
    Some(slot)
  }

  pub async fn lock_slot_for_committing_then_vacate(
    &self,
    mutation_writes: &mut Vec<(u64, Vec<u8>)>,
    slot_id: IncompleteSlotId,
    expected_object_id: u64,
  ) -> Option<RwLockWriteGuard<IncompleteSlot>> {
    let mut slot = self.slots.get(usz!(slot_id))?.write().await;
    if slot.is_vacant() || slot.object_id != expected_object_id {
      return None;
    };
    slot.vacate();
    // This is safe, as we'll hold the write lock while the return value hasn't been dropped, so even if another caller wants to acquire this one they won't be able to mutate the state yet.
    self.vacant_slot_ids.0.try_send(slot_id).unwrap();
    mutation_writes.push((INCOMPLETE_SLOTS_OFFSETOF_SLOT(slot_id), vec![
      0u8;
      usz!(
        INCOMPLETE_SLOT_SIZE
      )
    ]));
    Some(slot)
  }
}
