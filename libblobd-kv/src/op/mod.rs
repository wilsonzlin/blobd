use std::error::Error;
use std::fmt;
use std::fmt::Display;
use std::fmt::Write;

pub mod delete_object;
pub mod read_object;
pub mod write_object;

pub type OpResult<T> = Result<T, OpError>;

#[derive(Debug)]
pub enum OpError {
  ObjectNotFound,
  ObjectTooLarge,
  OutOfSpace,
  RangeOutOfBounds,
}

impl Display for OpError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      OpError::ObjectNotFound => write!(f, "object does not exist"),
      OpError::RangeOutOfBounds => write!(f, "requested range to read or write is invalid"),
      OpError::ObjectTooLarge => write!(f, "object is too large"),
      OpError::OutOfSpace => write!(f, "not enough free space"),
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
