use crate::object::id::ObjectIdSerial;
use crate::objects::CommittedObjects;
use crate::objects::IncompleteObjects;
use crate::pages::Pages;
use crate::state::StateWorker;
use crate::uring::UringBounded;

pub(crate) struct Ctx {
  pub committed_objects: CommittedObjects,
  pub device: UringBounded,
  pub incomplete_objects: IncompleteObjects,
  pub object_id_serial: ObjectIdSerial,
  pub pages: Pages,
  pub state: StateWorker,
}
