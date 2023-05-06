use std::error::Error;
use std::fmt;
use std::fmt::Display;
use std::fmt::Write;

pub mod commit_object;
pub mod create_object;
pub mod delete_object;
pub mod inspect_object;
pub mod read_object;
pub mod write_object;

pub type OpResult<T> = Result<T, OpError>;

#[derive(Debug)]
pub enum OpError {
  DataStreamError(Box<dyn Error + Send + Sync>),
  DataStreamLengthMismatch,
  InexactWriteLength,
  ObjectMetadataTooLarge,
  ObjectNotFound,
  RangeOutOfBounds,
  UnalignedWrite,
}

impl Display for OpError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      OpError::DataStreamError(e) => {
        write!(f, "an error occurred while reading the input data: {e}")
      }
      OpError::DataStreamLengthMismatch => write!(
        f,
        "the input data stream contains more or less bytes than specified"
      ),
      OpError::InexactWriteLength => write!(f, "data to write is not an exact chunk"),
      OpError::ObjectMetadataTooLarge => write!(f, "object metadata is too large"),
      OpError::ObjectNotFound => write!(f, "object does not exist"),
      OpError::RangeOutOfBounds => write!(f, "requested range to read or write is invalid"),
      OpError::UnalignedWrite => {
        write!(f, "data to write does not start at a multiple of TILE_SIZE")
      }
    }
  }
}

impl Error for OpError {}

#[allow(unused)]
pub(crate) fn key_debug_str(key: &[u8]) -> String {
  std::str::from_utf8(key)
    .map(|k| format!("lit:{k}"))
    .unwrap_or_else(|_| {
      let mut nice = "hex:".to_string();
      for (i, b) in key.iter().enumerate() {
        write!(nice, "{:02x}", b).unwrap();
        if i == 12 {
          nice.push('…');
          break;
        };
      }
      write!(nice, " ({})", key.len()).unwrap();
      nice
    })
}
