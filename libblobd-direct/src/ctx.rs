use crate::backing_store::PartitionStore;
use crate::object::id::ObjectIdSerial;
use crate::objects::CommittedObjects;
use crate::objects::IncompleteObjects;
use crate::pages::Pages;
use crate::state::StateWorker;

pub(crate) struct Ctx {
  pub committed_objects: CommittedObjects,
  pub device: PartitionStore,
  pub incomplete_objects: IncompleteObjects,
  pub object_id_serial: ObjectIdSerial,
  pub pages: Pages,
  pub state: StateWorker,
}
