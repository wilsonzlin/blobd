use crate::bucket::BucketsReader;
use crate::incomplete_list::IncompleteListReader;
use crate::state::StateWorker;
use crate::uring::Uring;

pub(crate) struct Ctx {
  pub buckets: BucketsReader,
  pub device: Uring,
  pub incomplete_list: IncompleteListReader,
  pub lpage_size_pow2: u8,
  pub spage_size_pow2: u8,
  pub state: StateWorker,
  pub versioning: bool,
}
