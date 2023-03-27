use std::error::Error;

pub mod commit_object;
pub mod create_object;
pub mod delete_object;
pub mod inspect_object;
pub mod read_object;
pub mod write_object;

pub type OpResult<T> = Result<T, OpError>;

pub enum OpError {
  DataStreamError(Box<dyn Error + Send + Sync>),
  DataStreamLengthMismatch,
  InexactWriteLength,
  ObjectNotFound,
  RangeOutOfBounds,
  UnalignedWrite,
}
